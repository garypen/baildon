//! B+Tree implementation
//!
//! This is the main data structure exposed by the library.
//!

use std::collections::HashMap;
use std::fmt::Display;
use std::io::ErrorKind;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use anyhow::Result;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use strum::EnumString;
use thiserror::Error;
use tokio::io;
use tokio::sync::{Mutex, MutexGuard};

use super::node::Node;
use super::sparse::BuildIdentityHasher;
use crate::command::Command;
use crate::io::file::BTreeFile;
use crate::io::wal::WalFile;

/// When accessing tree contents serially, ascending or descending order.
#[derive(Clone, Copy, Debug, EnumString, PartialEq)]
#[strum(ascii_case_insensitive)]
pub enum Direction {
    /// Process in ascending order.
    Ascending,
    /// Process in descending order.
    Descending,
}

const BAILDON_FILE_SIZE: u64 = 512_000;

/// Keys which we wish to store in a Baildon tree.
pub trait BaildonKey: Clone + Ord + Serialize + DeserializeOwned + std::fmt::Debug {}

// Blanket implementation which satisfies the compiler
impl<K> BaildonKey for K
where
    K: Clone + Ord + Serialize + DeserializeOwned + std::fmt::Debug,
{
    // Nothing to implement, since A already supports the other traits.
    // It has the functions it needs already
}

/// Values which we wish to store in a Baildon tree.
pub trait BaildonValue: Clone + Serialize + DeserializeOwned + std::fmt::Debug {}

// Blanket implementation which satisfies the compiler
impl<V> BaildonValue for V
where
    V: Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    // Nothing to implement, since A already supports the other traits.
    // It has the functions it needs already
}

/// Baildon specific errors.
#[derive(Error, Debug)]
pub enum BaildonError {
    /// Supplied branching factor too small
    #[error("branch: {0} must be >=2")]
    BranchTooSmall(u64),

    /// Could not find a node's child
    #[error("could not find child for node with index: {0}")]
    LostChild(usize),

    /// Could not find a node's parent
    #[error("could not find parent for node with index: {0}")]
    LostParent(usize),
}

/// A B+Tree.
pub struct Baildon<K, V>
// Constraints are required because Drop is implemented
where
    K: BaildonKey + Send + Sync,
    V: BaildonValue + Send + Sync,
{
    file: Mutex<BTreeFile>,
    path: PathBuf,
    root: Mutex<usize>,
    pub(crate) nodes: Mutex<HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
    branch: u64,
    pub(crate) index: AtomicUsize,
    wal: Mutex<WalFile>,
}

impl<K, V> Baildon<K, V>
where
    K: BaildonKey + Send + Sync,
    V: BaildonValue + Send + Sync,
{
    /// Create a new store at the specified path with the specified branching factor.
    pub async fn try_new<P: AsRef<Path>>(origin: P, branch: u64) -> Result<Self> {
        if branch < 2 {
            return Err(BaildonError::BranchTooSmall(branch).into());
        }
        let path: &Path = origin.as_ref();

        tracing::info!("Creating B+Tree at: {}", path.display());

        let mut file = BTreeFile::try_new(path, BAILDON_FILE_SIZE).await?;

        let root = Node::<K, V>::root(branch);

        let s_root = root.serialize()?;

        file.write_data(1, &s_root).await?;

        let mut nodes: HashMap<_, _, BuildIdentityHasher> = HashMap::default();
        nodes.insert(1, root);

        // If we can't create a new WalFile, we should fail because we might be trying to create a
        // store over a failed WAL. That will require manual clean up first.
        let mut wal_path = PathBuf::new();
        wal_path.push(origin.as_ref());
        wal_path.set_extension("wal");
        let wal = WalFile::try_new(&wal_path).await?;

        let this = Self {
            file: Mutex::new(file),
            path: path.into(),
            root: Mutex::new(1),
            nodes: Mutex::new(nodes),
            branch,
            index: AtomicUsize::new(2),
            wal: Mutex::new(wal),
        };
        this.inner_flush_to_disk(false).await?;
        Ok(this)
    }

    /// Open an exisiting store at the specified path.
    pub async fn try_open<P: AsRef<Path>>(origin: P) -> Result<Self> {
        let path: &Path = origin.as_ref();

        tracing::info!("Opening B+Tree at: {}", path.display());

        let mut file = BTreeFile::try_open(path).await?;

        let index = AtomicUsize::new(file.get_tree_index().await);

        let buf = file.read_data(file.get_root_index().await).await?;
        let root: Node<K, V> = Node::<K, V>::deserialize(&buf)?;
        let branch = root.branch();

        let mut nodes: HashMap<_, _, BuildIdentityHasher> = HashMap::default();
        let idx = root.index();
        nodes.insert(root.index(), root);

        // If we can open a WalFile, then we should replay it before allowing the open to complete
        // If not, last shutdown was fine, so create a new WalFile
        let mut wal_path = PathBuf::new();
        wal_path.push(origin.as_ref());
        wal_path.set_extension("wal");
        let mut recover = false;
        let wal = match WalFile::try_open(&wal_path).await {
            Ok(wal) => {
                recover = true;
                wal
            }
            Err(err) => {
                // If the error is NotFound, we can ignore the error since this is the happy path
                if let Some(io_error) = err.downcast_ref::<std::io::Error>() {
                    if io_error.kind() != ErrorKind::NotFound {
                        return Err(err);
                    }
                } else {
                    return Err(err);
                }
                WalFile::try_new(&wal_path).await?
            }
        };

        let this = Self {
            file: Mutex::new(file),
            path: path.into(),
            root: Mutex::new(idx),
            nodes: Mutex::new(nodes),
            branch,
            index,
            wal: Mutex::new(wal),
        };

        if recover {
            let mut wal = this.wal.lock().await;

            // Process wal file
            tracing::info!("Recovering from wal...");
            loop {
                match wal.read_data().await {
                    Ok(data) => {
                        let cmd: Command<K, V> = Command::deserialize(&data)?;
                        match cmd {
                            Command::Upsert(key, value) => {
                                // We don't care about the updated value, so ignore the
                                // function result
                                let _ = this.inner_insert(key, value).await;
                            }
                            Command::Delete(key) => {
                                // We don't care about the deleted value, so ignore the
                                // function result
                                let _ = this.inner_delete(&key).await;
                            }
                        }
                    }
                    Err(e) => {
                        // XXX This is perhaps a bit sketchy...
                        if let Some(down_e) = e.downcast_ref::<io::Error>() {
                            if down_e.kind() == io::ErrorKind::UnexpectedEof {
                                std::fs::remove_file(&wal_path)?;
                                *wal = WalFile::try_new(&wal_path).await?;
                                break;
                            }
                        }
                        tracing::info!("Recovering failed, data read error: {e:?}");
                        return Err(e);
                    }
                }
            }
            tracing::info!("Recovered!");
        }
        Ok(this)
    }

    /// Clear our tree.
    pub async fn clear(&self) -> Result<()> {
        let mut file_lock = self.file.lock().await;
        file_lock.reset(BAILDON_FILE_SIZE).await?;

        // Can't fail from here
        let mut nodes_lock = self.nodes.lock().await;
        nodes_lock.clear();
        self.index.store(1, Ordering::SeqCst);
        let root = Node::<K, V>::root(self.branch);
        self.add_node(&mut nodes_lock, root).await;
        let mut root_lock = self.root.lock().await;
        *root_lock = 1;
        Ok(())
    }

    /// Does the tree contain this key?
    pub async fn contains(&self, key: &K) -> bool {
        let mut nodes_lock = self.nodes.lock().await;
        let node = match self.search_node_with_lock(&mut nodes_lock, key).await {
            Ok(v) => v,
            Err(_) => return false,
        };
        node.key_index(key).is_some()
    }

    /// Return count of entries.
    pub async fn count(&self) -> usize {
        let count = AtomicUsize::new(0);
        let callback = |node: &Node<K, V>| {
            count.fetch_add(node.len(), Ordering::SeqCst);
            ControlFlow::Continue(())
        };
        self.traverse_leaf_nodes(Direction::Ascending, callback)
            .await;
        count.load(Ordering::SeqCst)
    }

    /// Delete a Key and return an optional previous Value.
    pub async fn delete(&self, key: &K) -> Result<Option<V>, anyhow::Error> {
        let cmd: Command<K, V> = Command::Delete(key.clone());
        let s_cmd = cmd.serialize()?;
        let mut wal_lock = self.wal.lock().await;
        wal_lock.write_data(&s_cmd).await?;
        self.inner_delete(key).await
    }

    async fn inner_delete(&self, key: &K) -> Result<Option<V>> {
        let mut nodes_lock = self.nodes.lock().await;

        let mut node = self.search_node_with_lock(&mut nodes_lock, key).await?;

        // REMEMBER if search_node() finds a node, we still need to confirm
        // that our node contains the key we are looking for.
        if node.key_index(key).is_none() {
            return Ok(None);
        }

        // XXX: This will return None if the key can't be found. Arguably, that's not quite the correct
        // logic, but correct enough for now.
        let value = node.remove_value(key);

        loop {
            if !node.is_minimum() {
                break;
            }
            // Process this node
            // Try to find a donor node
            let (neighbour_opt, direction) = match self
                .neighbour_same_parent_with_lock(
                    &mut nodes_lock,
                    node.index(),
                    Direction::Ascending,
                )
                .await
            {
                Some(n) => (Some(n), Direction::Ascending),
                None => (
                    self.neighbour_same_parent_with_lock(
                        &mut nodes_lock,
                        node.index(),
                        Direction::Descending,
                    )
                    .await,
                    Direction::Descending,
                ),
            };
            // Process this node
            match neighbour_opt {
                Some(mut neighbour) => {
                    // If our neighbour isn't at minimum we can simply take a k/v pair
                    if !neighbour.is_minimum() {
                        let p_idx = node
                            .parent()
                            .ok_or(BaildonError::LostParent(node.index()))?;
                        // Taking a key involves complex parent updates for both node and neighbour
                        // If taking from the Ascending:
                        //  - Take the first pair from the neighbour
                        //  - Push those onto our node
                        //  - Update our parent node entry key
                        // If taking from the Descending:
                        //  - Take the last pair from the neighbour
                        //  - Prepend those onto our node
                        //  - Update our parent neighbour entry key

                        // Update the neighbour
                        assert_eq!(neighbour.parent(), node.parent());
                        match &mut neighbour {
                            Node::Internal(data) => {
                                let (tgt_idx, (k, child)) = if direction == Direction::Ascending {
                                    (node.index(), (data.remove_pair(0)))
                                } else {
                                    (data.index(), (data.remove_pair(data.len() - 1)))
                                };

                                // Update our node
                                node.set_child(&k, child);

                                // Update the child (set its parent)
                                let closure = |child: &mut Node<K, V>| {
                                    child.set_parent(Some(node.index()));
                                    None
                                };
                                self.update_node(&mut nodes_lock, child, closure).await;

                                // Update the parent:
                                self.update_node(
                                    &mut nodes_lock,
                                    p_idx,
                                    |parent: &mut Node<K, V>| {
                                        parent.update_child_key(tgt_idx, k);
                                        None
                                    },
                                )
                                .await;
                            }
                            Node::Leaf(data) => {
                                let (tgt_idx, (k, value)) = if direction == Direction::Ascending {
                                    (node.index(), (data.remove_pair(0)))
                                } else {
                                    (data.index(), (data.remove_pair(data.len() - 1)))
                                };

                                // Update our node
                                node.set_value(&k, value);

                                // Update the parent:
                                self.update_node(
                                    &mut nodes_lock,
                                    p_idx,
                                    |parent: &mut Node<K, V>| {
                                        parent.update_child_key(tgt_idx, k);
                                        None
                                    },
                                )
                                .await;
                            }
                        }
                        // Replace our modified neighbour
                        self.replace_node(&mut nodes_lock, neighbour);
                    } else {
                        // We need to merge our neighbour
                        assert_ne!(neighbour.index(), node.index());
                        assert_eq!(neighbour.parent(), node.parent());
                        // Before we merge nodes, we need to make sure that every child has
                        // the same parent
                        if let Node::Internal(data) = &neighbour {
                            let p_idx = Some(node.index());
                            let closure_update_parent = move |child: &mut Node<K, V>| {
                                child.set_parent(p_idx);
                                None
                            };
                            for child in data.children() {
                                let _ = self
                                    .update_node(&mut nodes_lock, child, closure_update_parent)
                                    .await;
                            }
                        }
                        // Capture various useful bits of data before the merge
                        let neighbour_idx = neighbour.index();
                        let neighbour_max_key = neighbour.max_key().clone();
                        node.merge(neighbour);
                        // Update our parent
                        // We (may) need to adjust our parent to clean out our neighbour
                        let update_root = AtomicBool::new(false);
                        let closure_cleanup_parent = |parent: &mut Node<K, V>| {
                            // If we merged to the ascending:
                            //  - We take the max key from the neighbour and update our node key to
                            //    that value
                            //  - Remove the neighbour
                            // If we merged to the descending:
                            //  - Remove the neighbour
                            if direction == Direction::Ascending {
                                let _idx = parent.remove_child(neighbour_idx)?;
                                parent.update_child_key(node.index(), neighbour_max_key);
                            } else {
                                let _idx = parent.remove_child(neighbour_idx)?;
                            }

                            if parent.len() == 1 && parent.parent().is_none() {
                                assert_eq!(parent.len(), 1);
                                assert_eq!(
                                    parent.children().next().expect("HAS A 0"),
                                    node.index()
                                );
                                update_root.store(true, Ordering::SeqCst);
                            }
                            None
                        };
                        let p_idx = node
                            .parent()
                            .ok_or(BaildonError::LostParent(node.index()))?;
                        let _ = self
                            .update_node(&mut nodes_lock, p_idx, closure_cleanup_parent)
                            .await;
                        // Remove the lost node
                        nodes_lock.remove(&neighbour_idx);
                        // WE ARE VERY CAREFUL TO ONLY HOLD THE FILE LOCK BRIEFLY HERE
                        let mut file_lock = self.file.lock().await;
                        file_lock.free_data(neighbour_idx)?;

                        // Check if we need to update our root
                        if update_root.load(Ordering::SeqCst) {
                            let mut root_lock = self.root.lock().await;
                            *root_lock = node.index();
                            node.set_parent(None);
                            nodes_lock.remove(&p_idx);
                            file_lock.free_data(p_idx)?;
                            break;
                        }
                    }
                    let node_parent = node
                        .parent()
                        .ok_or(BaildonError::LostParent(node.index()))?;
                    // Replace our modified node
                    self.replace_node(&mut nodes_lock, node);
                    // Now, update our node for next loop
                    node = self
                        .find_node_with_lock(&mut nodes_lock, node_parent)
                        .await?;
                }
                // If we don't have a neighbour, we can't have a parent, so job done
                None => break,
            }
        }
        // Replace our modified node
        self.replace_node(&mut nodes_lock, node);
        Ok(value)
    }

    /// Serialize and store all our updated nodes to disk.
    pub async fn flush_to_disk(&self) -> Result<()> {
        self.inner_flush_to_disk(true).await
    }

    async fn inner_flush_to_disk(&self, remove_wal: bool) -> Result<()> {
        let mut nodes_lock = self.nodes.lock().await;
        let mut file_lock = self.file.lock().await;

        tracing::debug!("About to examine {} nodes", nodes_lock.len());
        for node in nodes_lock.values_mut().filter(|n| !n.clean()) {
            tracing::debug!("Storing node: {:?}", node);
            // Update root offset if required.
            if node.parent().is_none() {
                *self.root.lock().await = node.index();
                // *root_lock = node.index();
            }
            tracing::debug!("Storing dirty node {:?}", node);
            node.set_clean(true);
            let s_node = (*node).serialize()?;
            file_lock.write_data(node.index(), &s_node).await?;
        }
        // Update the file header
        let index = self.index.load(Ordering::SeqCst);
        file_lock
            .write_header_with_indices(*self.root.lock().await, index)
            .await?;

        tracing::debug!("Tree index: {}", self.index.load(Ordering::SeqCst));
        nodes_lock.clear();

        let result = file_lock.flush().await;
        if result.is_ok() && remove_wal {
            let mut wal_path = self.path.clone();
            wal_path.set_extension("wal");
            if let Err(e) = std::fs::remove_file(&wal_path) {
                tracing::error!("Error when removing WAL: {e}");
            }
        }
        result
    }

    /// Get the value.
    pub async fn get(&self, key: &K) -> Option<V> {
        let mut nodes_lock = self.nodes.lock().await;
        let node = self
            .search_node_with_lock(&mut nodes_lock, key)
            .await
            .ok()?;
        node.value(key)
    }

    /// Log basic information about our B+Tree.
    pub async fn info(&self) {
        tracing::info!(
            path = %self.path.display(),
            branching = self.branch,
            node_count = self.count().await,
            "B+Tree"
        );
    }

    /// Insert a Key and Value.
    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>, anyhow::Error> {
        let cmd = Command::Upsert(key.clone(), value.clone());
        let s_cmd = cmd.serialize()?;
        let mut wal_lock = self.wal.lock().await;
        wal_lock.write_data(&s_cmd).await?;
        Ok(self.inner_insert(key, value).await)
    }

    /// Insert a Key and Value.
    async fn inner_insert(&self, mut key: K, value: V) -> Option<V> {
        tracing::debug!("INSERTING: {:?}, {:?}", key, value);
        let mut nodes_lock = self.nodes.lock().await;

        let mut node = self
            .search_node_with_lock(&mut nodes_lock, &key)
            .await
            .ok()?;

        assert!(node.is_leaf());

        let value = node.set_value(&key, value);

        if node.is_full() {
            // Split the Node
            let new = node.split();
            key = node.max_key().clone();
            let mut new_key = new.max_key().clone();
            // Insert our new leaf node to the list of nodes
            let mut new_idx = self.add_node(&mut nodes_lock, new).await;
            loop {
                let p_opt = node.parent();
                match p_opt {
                    Some(p_idx) => {
                        // Help the borrow check by ensuring tmp will drop
                        let tmp_idx = node.index();
                        // Sync out our node and get ready to loop
                        self.replace_node(&mut nodes_lock, node);
                        // Process this parent
                        node = self
                            .find_node_as_option_with_lock(&mut nodes_lock, p_idx)
                            .await?;
                        node.set_child(&key, tmp_idx);
                        node.set_child(&new_key, new_idx);
                        if node.is_full() {
                            // Now split our node and prepare to add it next
                            // time around.
                            let new = node.split();
                            key = node.max_key().clone();
                            new_key = new.max_key().clone();
                            new_idx = self.add_node(&mut nodes_lock, new).await;
                        } else {
                            break;
                        }
                    }
                    None => {
                        let keys = vec![key, new_key];
                        let children = vec![node.index(), new_idx];
                        node.set_parent(Some(self.add_root(&mut nodes_lock, children, keys).await));
                        break;
                    }
                }
            }
        }
        // Finally, sync out our node and get ready to loop
        self.replace_node(&mut nodes_lock, node);
        value
    }

    /// Print to stdout all the nodes in the tree.
    pub async fn print_nodes(&self, direction: Direction) {
        let callback = |node: &Node<K, V>| {
            println!("node: {node:?}");
            ControlFlow::Continue(())
        };
        self.traverse_nodes(direction, callback).await
    }

    /// Traverse entries until stream exhausted or callback returns break.
    pub async fn traverse_entries(
        &self,
        direction: Direction,
        mut f: impl FnMut((K, V)) -> ControlFlow<()>,
    ) {
        let mut streamer = self.entries(direction).await;
        while let Some(entry) = streamer.next().await {
            match f(entry) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }
    }

    /// Traverse keys until stream exhausted or callback returns break.
    pub async fn traverse_keys(
        &self,
        direction: Direction,
        mut f: impl FnMut(K) -> ControlFlow<()>,
    ) {
        let mut streamer = self.keys(direction).await;
        while let Some(key) = streamer.next().await {
            match f(key) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }
    }

    /// Traverse values until stream exhausted or callback returns break.
    pub async fn traverse_values(
        &self,
        direction: Direction,
        mut f: impl FnMut(V) -> ControlFlow<()>,
    ) {
        let mut streamer = self.values(direction).await;
        while let Some(value) = streamer.next().await {
            match f(value) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }
    }

    /// Return leaf node utilization.
    pub async fn utilization(&self) -> f64 {
        let used = AtomicUsize::new(0);
        let total = AtomicUsize::new(0);

        let callback = |node: &Node<K, V>| {
            used.fetch_add(node.len(), Ordering::SeqCst);
            total.fetch_add(self.branch as usize, Ordering::SeqCst);
            ControlFlow::Continue(())
        };
        self.traverse_leaf_nodes(Direction::Ascending, callback)
            .await;
        used.load(Ordering::SeqCst) as f64 / total.load(Ordering::SeqCst) as f64
    }

    /// Verify all the nodes in the tree.
    pub async fn verify(&self, direction: Direction) -> Result<()> {
        let callback = |node: &Node<K, V>| {
            let mut seen_keys: Vec<K> = vec![];
            if node.is_leaf() {
                for key in node.keys() {
                    assert!(!seen_keys.contains(key));
                }
                seen_keys.extend(node.keys().cloned());
            } else {
                futures::executor::block_on(async {
                    let mut nodes_lock = self.nodes.lock().await;
                    for child in node.children() {
                        let child = match self.find_node_with_lock(&mut nodes_lock, child).await {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!("could not find node: {e}");
                                return ControlFlow::Break(());
                            }
                        };
                        assert_eq!(Some(node.index()), child.parent());
                    }
                    ControlFlow::Continue(())
                });
            }
            node.verify_keys();
            ControlFlow::Continue(())
        };
        self.traverse_nodes(direction, callback).await;
        Ok(())
    }

    /// Return last key.
    #[allow(dead_code)]
    async fn last_key(&self) -> Option<K> {
        self.last_leaf().await.keys().last().cloned()
    }

    /// Return first key.
    #[allow(dead_code)]
    async fn first_key(&self) -> Option<K> {
        self.first_leaf().await.keys().next().cloned()
    }

    /// Traverse all nodes in a tree using the callback.
    async fn traverse_nodes(
        &self,
        direction: Direction,
        mut f: impl FnMut(&Node<K, V>) -> ControlFlow<()>,
    ) {
        let mut streamer = self.stream_all_nodes(direction).await;
        while let Some(leaf) = streamer.next().await {
            match f(&leaf) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }
    }

    /// Traverse all leaf nodes in a tree using the callback.
    async fn traverse_leaf_nodes(
        &self,
        direction: Direction,
        mut f: impl FnMut(&Node<K, V>) -> ControlFlow<()>,
    ) {
        let mut streamer = self.stream_all_leaf_nodes(direction).await;
        while let Some(leaf) = streamer.next().await {
            match f(&leaf) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }
    }

    async fn add_node(
        &self,
        nodes_lock: &mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        mut node: Node<K, V>,
    ) -> usize {
        let idx = self.index.fetch_add(1, Ordering::SeqCst);
        node.set_index(idx);
        if let Node::Internal(data) = &node {
            for c_idx in data.children() {
                self.update_node(nodes_lock, c_idx, |node: &mut Node<K, V>| {
                    node.set_parent(Some(idx));
                    None
                })
                .await;
            }
        }
        nodes_lock.insert(idx, node);
        idx
    }

    fn replace_node(
        &self,
        nodes_lock: &mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        mut node: Node<K, V>,
    ) -> Option<Node<K, V>> {
        node.set_clean(false);
        nodes_lock.insert(node.index(), node)
    }

    async fn update_node(
        &self,
        nodes_lock: &mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        idx: usize,
        f: impl FnOnce(&mut Node<K, V>) -> Option<V>,
    ) -> Option<V> {
        // Add the node to our cache if it isn't already there
        if nodes_lock.get(&idx).is_none() {
            let node = self.read_node(idx).await.ok()?;
            nodes_lock.insert(idx, node);
        }
        let node = nodes_lock.get_mut(&idx).unwrap();
        tracing::debug!("Updating node: {:?}", node);
        // Always mark an updated node as not clean
        node.set_clean(false);
        f(node)
    }

    async fn add_root<'a>(
        &self,
        nodes_lock: &mut MutexGuard<'a, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        children: Vec<usize>,
        keys: Vec<K>,
    ) -> usize {
        tracing::debug!(
            "Adding a new root: children: {:?}, keys: {:?}",
            children,
            keys
        );
        let root: Node<K, V> = Node::internal(self.branch, None, keys, children.clone());
        let root_idx = self.add_node(nodes_lock, root).await;
        let closure = |node: &mut Node<K, V>| {
            node.set_parent(Some(root_idx));
            None
        };
        self.update_node(nodes_lock, children[0], closure).await;
        self.update_node(nodes_lock, children[1], closure).await;
        let mut root_lock = self.root.lock().await;
        *root_lock = root_idx;
        root_idx
    }

    /// Search our tree from the root for
    ///  - a leaf node to which our key will be added
    ///
    /// This will return the last node in the tree if an earlier node doesn't match first.
    #[inline]
    async fn search_node_with_lock(
        &self,
        nodes_lock: &'_ mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        key: &K,
    ) -> Result<Node<K, V>> {
        let mut target_node = self
            .find_node_with_lock(nodes_lock, *self.root.lock().await)
            .await?;
        loop {
            tracing::debug!("TARGET NODE: {:?}", target_node);
            if target_node.is_leaf() {
                return Ok(target_node.clone());
            }
            let t_idx = target_node
                .child(key)
                .ok_or(BaildonError::LostChild(target_node.index()))?;
            target_node = self.find_node_with_lock(nodes_lock, t_idx).await?;
        }
    }

    /// Find a node from cache (or disk).
    pub(crate) async fn find_node_as_option_with_lock(
        &self,
        nodes_lock: &'_ mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        idx: usize,
    ) -> Option<Node<K, V>> {
        match self.find_node_with_lock(nodes_lock, idx).await {
            Ok(n) => Some(n),
            Err(e) => {
                tracing::error!("could not find node with index {idx}: {e}");
                None
            }
        }
    }

    /// Find a node from cache (or disk).
    pub(crate) async fn find_node_with_lock(
        &self,
        nodes_lock: &'_ mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        idx: usize,
    ) -> Result<Node<K, V>> {
        let child = match nodes_lock.get(&idx) {
            Some(c) => c.clone(),
            None => {
                let node = self.read_node(idx).await?;
                nodes_lock.insert(idx, node.clone());
                node
            }
        };
        Ok(child)
    }

    /// Read a node from disk.
    async fn read_node(&self, idx: usize) -> Result<Node<K, V>> {
        let mut file_lock = self.file.lock().await;
        let buf = file_lock.read_data(idx).await?;
        Node::<K, V>::deserialize(&buf)
    }

    pub(crate) async fn first_leaf(&self) -> Node<K, V> {
        let mut nodes_lock = self.nodes.lock().await;
        let root_lock = self.root.lock().await;
        let mut node = self
            .find_node_with_lock(&mut nodes_lock, *root_lock)
            .await
            .expect("FLUFFED NODE");
        loop {
            if node.is_leaf() {
                return node;
            }
            let idx = node.first_child();
            node = self
                .find_node_with_lock(&mut nodes_lock, idx)
                .await
                .expect("FLUFFED NODE");
        }
    }

    pub(crate) async fn last_leaf(&self) -> Node<K, V> {
        let mut nodes_lock = self.nodes.lock().await;
        let root_lock = self.root.lock().await;
        let mut node = self
            .find_node_with_lock(&mut nodes_lock, *root_lock)
            .await
            .expect("FLUFFED NODE");
        loop {
            if node.is_leaf() {
                return node;
            }
            let idx = node.last_child();
            node = self
                .find_node_with_lock(&mut nodes_lock, idx)
                .await
                .expect("FLUFFED NODE");
        }
    }

    pub(crate) async fn neighbour(&self, idx: usize, direction: Direction) -> Option<Node<K, V>> {
        let mut nodes_lock = self.nodes.lock().await;
        self.neighbour_with_lock(&mut nodes_lock, idx, direction)
            .await
    }

    async fn neighbour_same_parent_with_lock(
        &self,
        nodes_lock: &'_ mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        idx: usize,
        direction: Direction,
    ) -> Option<Node<K, V>> {
        let node = self.find_node_as_option_with_lock(nodes_lock, idx).await?;
        match node.parent() {
            Some(p_idx) => {
                let parent = self
                    .find_node_as_option_with_lock(nodes_lock, p_idx)
                    .await?;

                // Can't have a neighbour if we have fewer than 2 children
                if parent.len() < 2 {
                    return None;
                }

                // Find my position
                let my_pos = parent.children().position(|x| x == idx)?;

                let neighbour_idx = if direction == Direction::Ascending {
                    // If last child, can't have a ascending neighbour
                    if my_pos == parent.len() - 1 {
                        return None;
                    }

                    // We know we can't get an error here
                    parent.children().nth(my_pos + 1)?
                } else {
                    // If first child, can't have a descending neighbour
                    if my_pos == 0 {
                        return None;
                    }
                    // We know we can't get an error here
                    parent.children().nth(my_pos - 1)?
                };
                self.find_node_as_option_with_lock(nodes_lock, neighbour_idx)
                    .await
            }
            None => None,
        }
    }

    async fn neighbour_with_lock(
        &self,
        nodes_lock: &'_ mut MutexGuard<'_, HashMap<usize, Node<K, V>, BuildIdentityHasher>>,
        mut idx: usize,
        direction: Direction,
    ) -> Option<Node<K, V>> {
        let mut node = self.find_node_as_option_with_lock(nodes_lock, idx).await?;

        let original_node = node.clone();

        // The logic for finding a neighbour is:
        // If we don't have a parent, we can't have a neighbour
        // If we do have a parent, check if we are the last child:
        //  - if we aren't - return the next child and then return
        //    this if it's a leaf or keep checking the first child until
        //    it is a leaf
        //  - if we are - keep looping up the tree and checking if we are
        //    the last child until either we aren't or we reach the top of
        //    the tree. If we aren't, do as first clause.

        loop {
            match node.parent() {
                Some(p_idx) => {
                    // Process this parent
                    node = self
                        .find_node_as_option_with_lock(nodes_lock, p_idx)
                        .await?;
                    // Not a direct descendent?
                    if !node.children().any(|x| x == idx) {
                        idx = p_idx;
                        continue;
                    }
                    if direction == Direction::Ascending {
                        // Is it the last child?
                        if node.last_child() == idx {
                            idx = p_idx;
                            continue;
                        }
                        // Get the next index
                        let c_pos = node.children().position(|x| x == idx)? + 1;

                        // Now keep looping down the children until we find a leaf node from c_pos
                        idx = node.children().nth(c_pos)?;
                        loop {
                            node = self.find_node_as_option_with_lock(nodes_lock, idx).await?;
                            if original_node.is_leaf() == node.is_leaf() {
                                break;
                            }
                            idx = node.children().next().unwrap();
                        }
                    } else {
                        // Is it the first child?
                        if node.first_child() == idx {
                            idx = p_idx;
                            continue;
                        }
                        // Get the previous index
                        let c_pos = node.children().position(|x| x == idx)? - 1;

                        // Now keep looping down the children until we find a node from c_pos
                        idx = node.children().nth(c_pos)?;
                        loop {
                            node = self.find_node_as_option_with_lock(nodes_lock, idx).await?;
                            if original_node.is_leaf() == node.is_leaf() {
                                break;
                            }
                            idx = node.children().last().unwrap();
                        }
                    }
                    return Some(node);
                }
                None => return None,
            }
        }
    }
}

impl<K, V> Baildon<K, V>
where
    K: BaildonKey + Send + Sync + Display,
    V: BaildonValue + Send + Sync + Display,
{
    /// Print to stdout all the keys and values in the tree.
    pub async fn print_entries(&self, direction: Direction) {
        let mut sep = "";
        let callback = |(key, value)| {
            print!("{sep}{key}:{value}");
            sep = ", ";
            ControlFlow::Continue(())
        };
        self.traverse_entries(direction, callback).await;
        println!();
    }

    /// Print to stdout all the keys in the tree.
    pub async fn print_keys(&self, direction: Direction) {
        let mut sep = "";
        let callback = |key| {
            print!("{sep}{key}");
            sep = ", ";
            ControlFlow::Continue(())
        };
        self.traverse_keys(direction, callback).await;
        println!();
    }

    /// Print to stdout all the values in the tree.
    pub async fn print_values(&self, direction: Direction) {
        let mut sep = "";
        let callback = |value| {
            print!("{sep}{value}");
            sep = ", ";
            ControlFlow::Continue(())
        };
        self.traverse_values(direction, callback).await;
        println!();
    }
}

impl<K, V> Drop for Baildon<K, V>
where
    K: BaildonKey + Send + Sync,
    V: BaildonValue + Send + Sync,
{
    fn drop(&mut self) {
        std::thread::scope(|s| {
            let hdl = s.spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
                if let Err(e) = runtime.block_on(self.flush_to_disk()) {
                    tracing::warn!("could not flush data file to disk: {}", e);
                }
            });
            hdl.join().expect("thread finished");
        });
    }
}

#[cfg(test)]
mod tests;
