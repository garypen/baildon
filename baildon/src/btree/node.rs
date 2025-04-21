//! B+Tree Node Types

use std::cmp::Ordering;

use anyhow::Error;
use anyhow::Result;
use bincode::Options;
use serde::{Deserialize, Serialize};

use super::baildon::BaildonKey;
use super::baildon::BaildonValue;
use crate::BINCODER;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct KeyPair<K, V> {
    key: K,
    value: V,
}

impl<K, V> PartialOrd for KeyPair<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> Ord for KeyPair<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> PartialEq for KeyPair<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K, V> Eq for KeyPair<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
}

impl<K, V> KeyPair<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    pub(crate) fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Node<K, V> {
    Internal(NodeInternal<K>),
    Leaf(NodeLeaf<K, V>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NodeLeaf<K, V> {
    pairs: Vec<KeyPair<K, V>>,
    branch: u64,
    parent: Option<usize>,
    idx: usize,
    clean: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NodeInternal<K> {
    pairs: Vec<KeyPair<K, usize>>,
    branch: u64,
    parent: Option<usize>,
    idx: usize,
    clean: bool,
}

impl<K, V> Node<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    pub(crate) fn root(branch: u64) -> Self {
        assert!(branch >= 2);
        let mut root = Node::leaf(
            branch,
            None,
            Vec::with_capacity(branch as usize),
            Vec::with_capacity(branch as usize),
        );
        root.set_index(1);
        root
    }

    fn leaf(branch: u64, parent: Option<usize>, keys: Vec<K>, values: Vec<V>) -> Self {
        assert!(branch >= 2);

        let mut pairs = Vec::with_capacity(branch as usize);
        for pair in std::iter::zip(keys, values) {
            pairs.push(KeyPair::new(pair.0, pair.1));
        }

        Node::Leaf(NodeLeaf {
            branch,
            parent,
            pairs,
            idx: 0,
            clean: false,
        })
    }

    fn leaf_from_pairs(branch: u64, parent: Option<usize>, pairs: Vec<KeyPair<K, V>>) -> Self {
        assert!(branch >= 2);

        Node::Leaf(NodeLeaf {
            branch,
            parent,
            pairs,
            idx: 0,
            clean: false,
        })
    }
    pub(crate) fn internal(
        branch: u64,
        parent: Option<usize>,
        keys: Vec<K>,
        children: Vec<usize>,
    ) -> Self {
        assert!(branch >= 2);
        assert!(keys.len() == children.len());

        let mut pairs = Vec::with_capacity(branch as usize);
        for pair in std::iter::zip(keys, children) {
            pairs.push(KeyPair::new(pair.0, pair.1));
        }

        Node::Internal(NodeInternal {
            branch,
            parent,
            pairs,
            idx: 0,
            clean: false,
        })
    }

    fn internal_from_pairs(
        branch: u64,
        parent: Option<usize>,
        pairs: Vec<KeyPair<K, usize>>,
    ) -> Self {
        assert!(branch >= 2);

        Node::Internal(NodeInternal {
            branch,
            parent,
            pairs,
            idx: 0,
            clean: false,
        })
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        BINCODER.serialize(self).map_err(Error::new)
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        BINCODER.deserialize(bytes).map_err(Error::new)
    }

    pub(crate) fn branch(&self) -> u64 {
        match self {
            Node::Internal(node) => node.branch,
            Node::Leaf(node) => node.branch,
        }
    }

    pub(crate) fn clean(&self) -> bool {
        match self {
            Node::Internal(node) => node.clean,
            Node::Leaf(node) => node.clean,
        }
    }

    pub(crate) fn key_index(&self, key: &K) -> Option<usize> {
        match self {
            Node::Internal(node) => node.pairs.binary_search_by(|pair| pair.key.cmp(key)).ok(),
            Node::Leaf(node) => node.pairs.binary_search_by(|pair| pair.key.cmp(key)).ok(),
        }
    }

    pub(crate) fn set_clean(&mut self, clean: bool) {
        match self {
            Node::Internal(node) => node.clean = clean,
            Node::Leaf(node) => node.clean = clean,
        }
    }

    pub(crate) fn index(&self) -> usize {
        match self {
            Node::Internal(node) => node.idx,
            Node::Leaf(node) => node.idx,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Node::Internal(node) => node.pairs.len(),
            Node::Leaf(node) => node.pairs.len(),
        }
    }

    pub(crate) fn set_index(&mut self, idx: usize) {
        match self {
            Node::Internal(node) => {
                node.clean = false;
                node.idx = idx;
            }
            Node::Leaf(node) => {
                node.clean = false;
                node.idx = idx;
            }
        }
    }

    pub(crate) fn parent(&self) -> Option<usize> {
        match self {
            Node::Internal(node) => node.parent,
            Node::Leaf(node) => node.parent,
        }
    }

    pub(crate) fn set_parent(&mut self, parent: Option<usize>) {
        match self {
            Node::Internal(node) => {
                node.clean = false;
                node.parent = parent;
            }
            Node::Leaf(node) => {
                node.clean = false;
                node.parent = parent;
            }
        }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        match self {
            Node::Internal(_) => false,
            Node::Leaf(_) => true,
        }
    }

    pub(crate) fn keys(&self) -> Box<dyn DoubleEndedIterator<Item = &K> + '_> {
        match self {
            Node::Internal(node) => Box::new(node.keys()),
            Node::Leaf(node) => Box::new(node.keys()),
        }
    }

    pub(crate) fn update_child_key(&mut self, idx: usize, new: K) -> Option<K> {
        match self {
            Node::Internal(node) => match node.pairs.iter().position(|x| x.value == idx) {
                Some(idx) => Some(std::mem::replace(&mut node.pairs[idx].key, new)),
                None => None,
            },
            Node::Leaf(_node) => panic!("Leaf nodes do not contain children"),
        }
    }

    pub(crate) fn values(&self) -> impl DoubleEndedIterator<Item = &V> + '_ {
        match self {
            Node::Internal(_node) => panic!("Internal nodes do not contain values"),
            Node::Leaf(node) => node.pairs.iter().map(|pair| &pair.value),
        }
    }

    pub(crate) fn pairs(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_ {
        match self {
            Node::Internal(_node) => panic!("Internal nodes do not contain values"),
            Node::Leaf(node) => node.pairs.iter().map(|pair| (&pair.key, &pair.value)),
        }
    }

    pub(crate) fn children(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        match self {
            Node::Internal(node) => node.pairs.iter().map(|pair| pair.value),
            Node::Leaf(_node) => panic!("Leaf nodes do not contain children"),
        }
    }

    /// Return a node for a key. If the key doesn't exist, we'll still return a node, so this value
    /// always returns a child node (as long as there are children).
    pub(crate) fn child(&self, key: &K) -> Option<usize> {
        match self {
            Node::Internal(node) => match node.pairs.binary_search_by(|pair| pair.key.cmp(key)) {
                Ok(idx) => Some(node.pairs[idx].value),
                Err(idx) => {
                    if idx == node.pairs.len() {
                        Some(node.pairs[idx - 1].value)
                    } else {
                        Some(node.pairs[idx].value)
                    }
                }
            },
            Node::Leaf(_node) => panic!("Leaf nodes do not contain children"),
        }
    }

    pub(crate) fn remove_child(&mut self, idx: usize) -> Option<usize> {
        match self {
            Node::Internal(node) => match node.pairs.iter().position(|x| x.value == idx) {
                Some(idx) => {
                    node.clean = false;
                    Some(node.pairs.remove(idx).value)
                }
                None => None,
            },
            Node::Leaf(_node) => panic!("Leaf nodes do not contain children"),
        }
    }

    pub(crate) fn set_child(&mut self, key: &K, child: usize) -> Option<usize> {
        match self {
            Node::Internal(node) => match node.pairs.binary_search_by(|pair| pair.key.cmp(key)) {
                Ok(idx) => {
                    let old = Some(node.pairs[idx].value);
                    node.pairs[idx].value = child;
                    node.clean = false;
                    old
                }
                Err(idx) => {
                    // If we can't find the index or the index value doesn't match our child,
                    // allocate new KeyPair and insert it.
                    if idx == 0 || idx == node.pairs.len() || node.pairs[idx].value != child {
                        let pair = KeyPair::new(key.clone(), child);
                        node.pairs.insert(idx, pair);
                        node.clean = false;
                        None
                    } else {
                        let old = Some(node.pairs[idx].value);
                        node.pairs[idx].key = key.clone();
                        node.pairs[idx].value = child;
                        node.clean = false;
                        old
                    }
                }
            },
            Node::Leaf(_node) => panic!("Leaf nodes do not contain children"),
        }
    }

    pub(crate) fn value(&self, key: &K) -> Option<V> {
        match self {
            Node::Internal(_node) => panic!("Internal nodes do not contain values"),
            Node::Leaf(node) => node
                .pairs
                .iter()
                .find(|pair| pair.key == *key)
                .map(|x| x.value.clone()),
        }
    }

    pub(crate) fn remove_value(&mut self, key: &K) -> Option<V> {
        match self {
            Node::Internal(_node) => panic!("Internal nodes do not contain values"),
            Node::Leaf(node) => match node.pairs.binary_search_by(|pair| pair.key.cmp(key)) {
                Ok(idx) => {
                    node.clean = false;
                    Some(node.pairs.remove(idx).value)
                }
                Err(_) => None,
            },
        }
    }

    pub(crate) fn set_value(&mut self, key: &K, value: V) -> Option<V> {
        match self {
            Node::Internal(_node) => panic!("Internal nodes do not contain values"),
            Node::Leaf(node) => {
                node.clean = false;
                match node.pairs.binary_search_by(|pair| pair.key.cmp(key)) {
                    Ok(idx) => Some(std::mem::replace(&mut node.pairs[idx].value, value)),
                    Err(idx) => {
                        let pair = KeyPair::new(key.clone(), value);
                        node.pairs.insert(idx, pair);
                        None
                    }
                }
            }
        }
    }

    pub(crate) fn max_key(&self) -> &K {
        match self {
            Node::Internal(node) => &node.pairs.last().unwrap().key,
            Node::Leaf(node) => &node.pairs.last().unwrap().key,
        }
    }

    // Internal nodes must have children, so this is ok
    pub(crate) fn first_child(&self) -> usize {
        match self {
            Node::Internal(node) => node.pairs.first().expect("THERE IS A FIRST").value,
            Node::Leaf(_node) => panic!("Leaf does not contain children"),
        }
    }

    // Internal nodes must have children, so this is ok
    pub(crate) fn last_child(&self) -> usize {
        match self {
            Node::Internal(node) => node.pairs.last().expect("THERE IS A LAST").value,
            Node::Leaf(_node) => panic!("Leaf does not contain children"),
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        match self {
            Node::Internal(node) => node.pairs.len() as u64 > node.branch,
            Node::Leaf(node) => node.pairs.len() as u64 > node.branch,
        }
    }

    pub(crate) fn is_minimum(&self) -> bool {
        match self {
            Node::Internal(node) => {
                if node.parent.is_none() {
                    node.pairs.is_empty()
                } else {
                    (node.pairs.len() as u64) < node.branch / 2
                }
            }
            Node::Leaf(node) => {
                if node.parent.is_none() {
                    node.pairs.is_empty()
                } else {
                    (node.pairs.len() as u64) < node.branch / 2
                }
            }
        }
    }

    pub(crate) fn split(&mut self) -> Node<K, V> {
        match self {
            Node::Internal(node) => {
                let split = (node.branch / 2 + node.branch % 2) as usize;

                tracing::debug!("Splitting internal node: {:?}, split: {}", node, split);
                let new = Node::internal_from_pairs(
                    node.branch,
                    node.parent,
                    node.pairs.split_off(split),
                );
                node.clean = false;
                tracing::debug!("After split: node: {:?}", node);
                tracing::debug!("After split: new: {:?}", new);
                assert!((node.pairs.len() as u64) >= node.branch / 2);
                new
            }
            Node::Leaf(node) => {
                let split = (node.branch / 2 + node.branch % 2) as usize;

                tracing::debug!("SPLITTING LEAF NODE: {:?}, split: {}", node, split);
                let new =
                    Node::leaf_from_pairs(node.branch, node.parent, node.pairs.split_off(split));
                node.clean = false;
                tracing::debug!("After split: node: {:?}", node);
                tracing::debug!("After split: new: {:?}", new);
                new
            }
        }
    }

    pub(crate) fn merge(&mut self, other: Node<K, V>) {
        match self {
            Node::Internal(node) => match other {
                Node::Internal(node_other) => {
                    assert_eq!(node.branch, node_other.branch);
                    if node.pairs[0].key < node_other.pairs[0].key {
                        node.pairs.extend(node_other.pairs);
                    } else {
                        node.pairs.splice(0..0, node_other.pairs);
                    }
                    node.clean = false;
                }
                Node::Leaf(_node_other) => {
                    panic!("Cannot merge Internal node with a Leaf node")
                }
            },
            Node::Leaf(node) => match other {
                Node::Internal(_node_other) => {
                    panic!("Cannot merge Leaf node with an Internal node")
                }
                Node::Leaf(node_other) => {
                    assert_eq!(node.branch, node_other.branch);
                    if node.pairs[0].key < node_other.pairs[0].key {
                        node.pairs.extend(node_other.pairs);
                    } else {
                        node.pairs.splice(0..0, node_other.pairs);
                    }
                    node.clean = false;
                }
            },
        }
        assert!(!self.is_full());
    }

    pub(crate) fn verify_keys(&self) {
        let mut previous = None;
        match self {
            Node::Internal(node) => {
                for pair in &node.pairs {
                    let key = &pair.key;
                    match previous {
                        Some(k) => {
                            assert!(k < key);
                            previous = Some(key);
                        }
                        None => previous = Some(key),
                    }
                }
            }
            Node::Leaf(node) => {
                for pair in &node.pairs {
                    let key = &pair.key;
                    match previous {
                        Some(k) => {
                            assert!(k < key);
                            previous = Some(key);
                        }
                        None => previous = Some(key),
                    }
                }
            }
        }
    }
}

impl<K> NodeInternal<K>
where
    K: BaildonKey,
{
    pub(crate) fn keys(&self) -> impl DoubleEndedIterator<Item = &K> + '_ {
        self.pairs.iter().map(|pair| &pair.key)
    }

    pub(crate) fn index(&self) -> usize {
        self.idx
    }

    pub(crate) fn len(&self) -> usize {
        self.pairs.len()
    }

    pub(crate) fn children(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        self.pairs.iter().map(|pair| pair.value)
    }

    pub(crate) fn remove_pair(&mut self, idx: usize) -> (K, usize) {
        let pair = self.pairs.remove(idx);
        self.clean = false;
        (pair.key, pair.value)
    }
}

impl<K, V> NodeLeaf<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    pub(crate) fn keys(&self) -> impl DoubleEndedIterator<Item = &K> + '_ {
        self.pairs.iter().map(|pair| &pair.key)
    }

    pub(crate) fn index(&self) -> usize {
        self.idx
    }

    pub(crate) fn len(&self) -> usize {
        self.pairs.len()
    }

    pub(crate) fn remove_pair(&mut self, idx: usize) -> (K, V) {
        let pair = self.pairs.remove(idx);
        self.clean = false;
        (pair.key, pair.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_finds_a_child() {
        let children = vec![1usize, 2, 3];
        let target: Node<String, usize> = Node::internal(
            8,
            None,
            vec!["b".to_string(), "d".to_string(), "f".to_string()],
            children.clone(),
        );
        let search_keys = [
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
        ];

        for (index, key) in search_keys.iter().enumerate() {
            assert_eq!(children[index / 2], target.child(key).unwrap());
        }
    }

    #[test]
    fn it_merges_leaf_nodes() {
        let mut target = Node::leaf(
            8,
            None,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec![1usize, 3, 5],
        );
        let source = Node::leaf(
            8,
            None,
            vec!["d".to_string(), "e".to_string(), "f".to_string()],
            vec![2usize, 4, 6],
        );
        target.merge(source);
        assert_eq!(
            target.keys().collect::<Vec<&String>>(),
            vec!["a", "b", "c", "d", "e", "f"]
        );
        assert_eq!(
            target.values().cloned().collect::<Vec<usize>>(),
            vec![1usize, 3, 5, 2, 4, 6]
        );
    }

    #[test]
    #[should_panic]
    fn it_wont_merge_leaf_nodes_when_full() {
        let mut target = Node::leaf(
            5,
            None,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec![1usize, 3, 5],
        );
        let source = Node::leaf(
            5,
            None,
            vec!["d".to_string(), "e".to_string(), "f".to_string()],
            vec![2usize, 4, 6],
        );
        target.merge(source);
        assert_eq!(
            target.keys().collect::<Vec<&String>>(),
            vec!["a", "b", "c", "d", "e", "f"]
        );
        assert_eq!(
            target.values().cloned().collect::<Vec<usize>>(),
            vec![1usize, 3, 5, 2, 4, 6]
        );
    }
}
