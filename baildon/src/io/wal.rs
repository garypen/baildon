//! WAL functions
//!

use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub(crate) struct WalFile {
    file: File,
    sync_allowed: Arc<AtomicBool>,
}

impl WalFile {
    pub(crate) async fn try_open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .open(path)
            .await?;

        Ok(Self {
            file,
            sync_allowed: Arc::new(AtomicBool::default()),
        })
    }

    pub(crate) async fn try_new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)
            .await?;

        let sync_allowed = Arc::new(AtomicBool::default());
        let shared_sync = sync_allowed.clone();
        tokio::spawn(async move {
            // Re-enable flushing every 2 seconds
            let mut timer = tokio::time::interval(Duration::from_secs(2));
            loop {
                timer.tick().await;
                shared_sync.store(true, Ordering::Release);
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(()) // <- note the explicit type annotation here
        });
        Ok(Self { file, sync_allowed })
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        // To prevent excessive flushing, we only flush if our allowed flag is true
        if self
            .sync_allowed
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            self.file.sync_all().await.map_err(|e| e.into())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn write_data(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_u64(data.len() as u64).await?;
        self.file.write_all(data).await?;
        self.flush().await
    }

    pub(crate) async fn read_data(&mut self) -> Result<Vec<u8>> {
        let len = self.file.read_u64().await?;
        let mut buf = vec![0; len as usize];
        let _ = self.file.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::command::Command;

    #[tokio::test]
    async fn it_creates_wal_file() {
        let _wal = WalFile::try_new(Path::new("wal_file_create.db"))
            .await
            .expect("creates wal file");
        std::fs::remove_file("wal_file_create.db").expect("cleanup");
    }

    #[tokio::test]
    async fn it_opens_wal_file() {
        let mut wal = WalFile::try_new(Path::new("wal_file_open.db"))
            .await
            .expect("creates wal file");
        wal.flush().await.expect("flushed away");
        drop(wal);
        let _wal = WalFile::try_open(Path::new("wal_file_open.db"))
            .await
            .expect("opens wal file");
        std::fs::remove_file("wal_file_open.db").expect("cleanup");
    }

    #[tokio::test]
    async fn it_writes_to_wal_file() {
        let mut wal = WalFile::try_new(Path::new("wal_file_write.db"))
            .await
            .expect("creates wal file");
        let upsert = Command::Upsert("key".to_string(), "value".to_string());
        let data = upsert.serialize().expect("serializes");
        wal.write_data(&data).await.expect("write data");

        drop(wal);

        let mut wal = WalFile::try_open(Path::new("wal_file_write.db"))
            .await
            .expect("opens wal file");
        let new_upsert = Command::deserialize(&wal.read_data().await.expect("reads data"))
            .expect("deserializes");
        assert_eq!(upsert, new_upsert);
        std::fs::remove_file("wal_file_write.db").expect("cleanup");
    }
}
