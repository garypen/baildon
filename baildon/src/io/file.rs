//! File functions
//!
//! The file has the following structure
//!
//! Header
//!   [Block]
//! Footer
//!
//! The Header contains a couple of useful indices and the offeset of the Footer.
//! The Footer contains:
//!   Blocks are the blocks of data used to store Nodes. `VecDeque<Block>`
//!   BlockMap associates an index with a Block `HashMap<Index, Block>`

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::io::SeekFrom;
use std::path::Path;

use anyhow::Result;
use bincode::Options;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::BINCODER;

const BLOCK_SIZE: u64 = 512;

const FORMAT_VERSION_1: u8 = 1;

const SUPPORTED_VERSIONS: &[u8] = &[FORMAT_VERSION_1];

#[derive(Debug)]
pub(crate) struct BTreeFile {
    file: File,
    header: BTreeFileHeader,
    footer: BTreeFileFooter,
}

#[derive(Debug, Serialize, Deserialize)]
struct BTreeFileFooter {
    map_size: u64,
    block_map: HashMap<usize, Block>,
    blocks_size: u64,
    blocks: VecDeque<Block>,
}
#[derive(Debug, Serialize, Deserialize)]
struct BTreeFileHeader {
    version: u8,
    footer_offset: u64,
    root_index: usize,
    tree_index: usize,
}

#[derive(Error, Debug)]
pub enum BTreeFileError {
    #[error("could not insert block at index: {0}")]
    BlockReturn(usize),
    #[error("could not find block at pos: {0}")]
    LostBlock(usize),
    #[error("could not find block mapping for index: {0}")]
    LostMapping(usize),
    #[error("file version not supported: {0}")]
    InvalidFileVersion(u8),
}

/// A Block of storage
#[derive(Debug, Eq, Serialize, Deserialize)]
pub(crate) struct Block {
    /// Offset within file
    offset: u64,
    /// Number of BLOCK_SIZE chunks in block
    count: u64,
}

impl Block {
    fn split(&mut self, count: u64) -> Option<Block> {
        assert!(count <= self.count);
        if count < self.count {
            let rem = Block {
                offset: self.offset + count * BLOCK_SIZE,
                count: self.count - count,
            };
            self.count = count;
            Some(rem)
        } else {
            None
        }
    }
}

impl Ord for Block {
    fn cmp(&self, other: &Self) -> Ordering {
        self.count.cmp(&other.count)
    }
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
    }
}

impl BTreeFile {
    pub(crate) async fn try_open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
            .await?;

        let header = BTreeFile::read_header(&mut file).await?;

        if !SUPPORTED_VERSIONS.contains(&header.version) {
            return Err(BTreeFileError::InvalidFileVersion(header.version).into());
        }

        let footer = BTreeFile::read_footer(&mut file, header.footer_offset).await?;

        Ok(Self {
            file,
            header,
            footer,
        })
    }

    pub(crate) async fn try_new(path: &Path, size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;

        file.set_len(512_584).await?;

        let mut blocks = VecDeque::new();

        let (header, block) = BTreeFile::create_file_artifacts(size);

        blocks.push_front(block);

        let block_map = HashMap::new();

        let footer = BTreeFileFooter {
            map_size: BINCODER.serialized_size(&block_map)?,
            block_map,
            blocks_size: BINCODER.serialized_size(&blocks)?,
            blocks,
        };

        Ok(Self {
            file,
            header,
            footer,
        })
    }

    pub(crate) async fn reset(&mut self, size: u64) -> Result<()> {
        self.file.set_len(512_584).await?;
        self.footer.block_map.clear();
        self.footer.blocks.clear();

        let (header, block) = BTreeFile::create_file_artifacts(size);

        self.footer.blocks.push_front(block);

        self.footer.map_size = BINCODER.serialized_size(&self.footer.block_map)?;
        self.footer.blocks_size = BINCODER.serialized_size(&self.footer.blocks)?;

        self.header = header;

        Ok(())
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        self.file.sync_all().await.map_err(|e| e.into())
    }

    pub(crate) async fn read_data(&mut self, index: usize) -> Result<Vec<u8>> {
        match self.footer.block_map.get(&index) {
            Some(block) => {
                let mut buf = vec![0; (BLOCK_SIZE * block.count) as usize];
                self.file.seek(SeekFrom::Start(block.offset)).await?;
                self.file.read_exact(&mut buf).await?;
                Ok(buf)
            }
            None => Err(BTreeFileError::LostMapping(index).into()),
        }
    }

    pub(crate) fn free_data(&mut self, index: usize) -> Result<()> {
        self.footer
            .block_map
            .remove(&index)
            .map(|block| {
                let pos = self
                    .footer
                    .blocks
                    .partition_point(|x| block.count <= x.count);
                self.footer.blocks.insert(pos, block);
            })
            .ok_or(BTreeFileError::LostMapping(index).into())
    }

    pub(crate) async fn write_data(&mut self, index: usize, data: &[u8]) -> Result<()> {
        // Somewhat unusual structure because we may have to migrate a data block
        let offset = match self.footer.block_map.get(&index) {
            Some(block) => {
                let count = BTreeFile::blocks_needed(data.len() as u64);
                if count > block.count {
                    // Need to migrate
                    let new_block = self.get_block(data.len() as u64).await?;
                    let offset = new_block.offset;
                    let old_block = self
                        .footer
                        .block_map
                        .insert(index, new_block)
                        .ok_or(BTreeFileError::BlockReturn(index))?;
                    // .expect("must already be a value; qed");
                    // Return old block into blocks...
                    let pos = self
                        .footer
                        .blocks
                        .partition_point(|x| old_block.count <= x.count);
                    self.footer.blocks.insert(pos, old_block);
                    offset
                } else {
                    block.offset
                }
            }
            None => {
                let block = self.get_block(data.len() as u64).await?;
                let offset = block.offset;
                self.footer.block_map.insert(index, block);
                offset
            }
        };
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(data).await?;
        Ok(())
    }

    pub(crate) async fn get_root_index(&self) -> usize {
        self.header.root_index
    }

    pub(crate) async fn get_tree_index(&self) -> usize {
        self.header.tree_index
    }

    async fn read_header(file: &mut File) -> Result<BTreeFileHeader> {
        let mut buf = vec![0; BLOCK_SIZE as usize];

        file.seek(SeekFrom::Start(0)).await?;
        file.read_exact(&mut buf).await?;

        BINCODER.deserialize(&buf).map_err(|e| e.into())
    }

    async fn read_footer(file: &mut File, offset: u64) -> Result<BTreeFileFooter> {
        file.seek(SeekFrom::Start(offset)).await?;

        let mut size_buf = vec![0; 8];

        let _map_size = file.read_exact(&mut size_buf).await?;
        let map_size: u64 = BINCODER.deserialize(&size_buf)?;

        let mut map_buf = vec![0; map_size as usize];

        let _block_map = file.read_exact(&mut map_buf).await?;
        let block_map = BINCODER.deserialize(&map_buf)?;

        let _blocks_size = file.read_exact(&mut size_buf).await?;
        let blocks_size: u64 = BINCODER.deserialize(&size_buf)?;

        let mut blocks_buf = vec![0; blocks_size as usize];

        let _blocks = file.read_exact(&mut blocks_buf).await?;
        let blocks = BINCODER.deserialize(&blocks_buf)?;

        Ok(BTreeFileFooter {
            map_size: BINCODER.serialized_size(&block_map)?,
            block_map,
            blocks_size: BINCODER.serialized_size(&blocks)?,
            blocks,
        })
    }

    pub(crate) async fn write_header_with_indices(
        &mut self,
        root_index: usize,
        tree_index: usize,
    ) -> Result<()> {
        self.header.root_index = root_index;
        self.header.tree_index = tree_index;
        self.write_header_and_footer().await
    }

    async fn write_header_and_footer(&mut self) -> Result<()> {
        let s_header = BINCODER.serialize(&self.header)?;
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&s_header).await?;

        let s_map = BINCODER.serialize(&self.footer.block_map)?;
        let s_blocks = BINCODER.serialize(&self.footer.blocks)?;
        self.footer.map_size = BINCODER.serialized_size(&self.footer.block_map)?;
        self.footer.blocks_size = BINCODER.serialized_size(&self.footer.blocks)?;
        let s_map_size = BINCODER.serialize(&self.footer.map_size)?;
        let s_blocks_size = BINCODER.serialize(&self.footer.blocks_size)?;
        self.file
            .seek(SeekFrom::Start(self.header.footer_offset))
            .await?;

        self.file.write_all(&s_map_size).await?;
        self.file.write_all(&s_map).await?;
        self.file.write_all(&s_blocks_size).await?;
        self.file.write_all(&s_blocks).await?;

        Ok(())
    }

    /// Initialise our file structure based on desired storage space
    fn create_file_artifacts(size: u64) -> (BTreeFileHeader, Block) {
        let count = BTreeFile::blocks_needed(size);

        // Add on a block to store the header in
        let hdr = BTreeFileHeader {
            version: FORMAT_VERSION_1,
            footer_offset: (count + 1) * BLOCK_SIZE,
            root_index: 1,
            tree_index: 2,
        };

        let block = Block {
            offset: BLOCK_SIZE,
            count,
        };

        (hdr, block)
    }

    fn blocks_needed(size: u64) -> u64 {
        if size % BLOCK_SIZE == 0 {
            size / BLOCK_SIZE
        } else {
            size / BLOCK_SIZE + 1
        }
    }

    /// Get (or allocate) a block to write with
    async fn get_block(&mut self, size: u64) -> Result<Block> {
        // Search our list of existing blocks to find a block that is >= required size (in bytes).
        // If we can't find a block, we need to expand our file,
        let count = BTreeFile::blocks_needed(size);
        let mut pos = self.footer.blocks.partition_point(|x| count <= x.count);
        if pos == 0 {
            // TODO: We could do some coalescing here first...
            // Add a block to the front which is the size requested.
            // TODO: Perhaps we should constrain that limit...
            // TODO: Consider just expanding by a minimum of 1MB or amount requested.
            let block = Block {
                offset: self.header.footer_offset,
                count,
            };
            self.header.footer_offset += block.count * BLOCK_SIZE;
            // Write out our updated header and footer maps
            self.write_header_and_footer().await?;
            // .expect("NEED TO HANDLE THIS AT SOME POINT");
            self.footer.blocks.push_front(block);
            pos = self.footer.blocks.partition_point(|x| count <= x.count);
        }
        // Take 1 away from pos to get the last valid value (that's what we are looking for)
        pos -= 1;
        // At this point, pos must be a valid value. Let's split that block and then re-insert it
        self.footer
            .blocks
            .remove(pos)
            .map(|mut block| {
                let rem_opt = block.split(count);
                if let Some(rem) = rem_opt {
                    let pos = self.footer.blocks.partition_point(|x| rem.count <= x.count);
                    self.footer.blocks.insert(pos, rem);
                }
                block
            })
            .ok_or(BTreeFileError::LostBlock(pos).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_creates_btree_file() {
        let _tree = BTreeFile::try_new(Path::new("file_create.db"), 1_024)
            .await
            .expect("creates tree file");
        std::fs::remove_file("file_create.db").expect("cleanup");
    }

    #[tokio::test]
    async fn it_opens_btree_file() {
        let mut tree = BTreeFile::try_new(Path::new("file_open.db"), 1_024)
            .await
            .expect("creates tree file");
        tree.write_header_with_indices(tree.get_root_index().await, tree.get_tree_index().await)
            .await
            .expect("header written");
        tree.flush().await.expect("flushed away");
        drop(tree);
        let _tree = BTreeFile::try_open(Path::new("file_open.db"))
            .await
            .expect("opens tree file");
        std::fs::remove_file("file_open.db").expect("cleanup");
    }

    #[tokio::test]
    async fn it_finds_block() {
        let mut tree = BTreeFile::try_new(Path::new("file_find_valid_block.db"), 1_024)
            .await
            .expect("creates tree file");
        tree.get_block(20482).await.expect("gets a block");
        tree.get_block(513).await.expect("gets a block");
        std::fs::remove_file("file_find_valid_block.db").expect("cleanup");
    }
}
