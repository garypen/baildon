use std::sync::atomic::Ordering;

use super::baildon::Baildon;
use super::baildon::Direction;
use super::node::Node;

use futures::stream;
use futures::Stream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

impl<K, V> Baildon<K, V>
where
    K: Clone + Ord + Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync,
{
    /// Return a stream of entries
    pub async fn entries(&self, direction: Direction) -> impl Stream<Item = (K, V)> + '_ {
        let mut streamer = self.stream_all_leaf_nodes(direction).await;
        let index = 0;
        let leaf_opt = streamer.next().await;

        // Each node contains a number of values, we must read all the values from the current node
        // before advancing. (i.e.: a loop within a loop)
        Box::pin(stream::unfold(
            (streamer, leaf_opt, index),
            move |mut triplet| async move {
                loop {
                    match &triplet.1 {
                        Some(leaf) => {
                            let pair_opt = if direction == Direction::Ascending {
                                leaf.pairs().nth(triplet.2)
                            } else {
                                leaf.pairs().rev().nth(triplet.2)
                            };
                            match pair_opt {
                                Some(pair) => {
                                    triplet.2 += 1;
                                    break Some(((pair.0.clone(), pair.1.clone()), triplet));
                                }
                                None => {
                                    triplet.1 = triplet.0.next().await;
                                    triplet.2 = 0;
                                    continue;
                                }
                            }
                        }
                        None => break None,
                    };
                }
            },
        ))
    }

    /// Return a stream of keys
    pub async fn keys(&self, direction: Direction) -> impl Stream<Item = K> + '_ {
        let mut streamer = self.stream_all_leaf_nodes(direction).await;
        let index = 0;
        let leaf_opt = streamer.next().await;

        // Each node contains a number of keys, we must read all the keys from the current node
        // before advancing. (i.e.: a loop within a loop)
        Box::pin(stream::unfold(
            (streamer, leaf_opt, index),
            move |mut triplet| async move {
                loop {
                    match &triplet.1 {
                        Some(leaf) => {
                            let key_opt = if direction == Direction::Ascending {
                                leaf.keys().nth(triplet.2)
                            } else {
                                leaf.keys().rev().nth(triplet.2)
                            };
                            match key_opt {
                                Some(key) => {
                                    triplet.2 += 1;
                                    break Some(((*key).clone(), triplet));
                                }
                                None => {
                                    triplet.1 = triplet.0.next().await;
                                    triplet.2 = 0;
                                    continue;
                                }
                            }
                        }
                        None => break None,
                    }
                }
            },
        ))
    }

    /// Return a stream of values
    pub async fn values(&self, direction: Direction) -> impl Stream<Item = V> + '_ {
        let mut streamer = self.stream_all_leaf_nodes(direction).await;
        let index = 0;
        let leaf_opt = streamer.next().await;

        // Each node contains a number of values, we must read all the values from the current node
        // before advancing. (i.e.: a loop within a loop)
        Box::pin(stream::unfold(
            (streamer, leaf_opt, index),
            move |mut triplet| async move {
                loop {
                    match &triplet.1 {
                        Some(leaf) => {
                            let value_opt = if direction == Direction::Ascending {
                                leaf.values().nth(triplet.2)
                            } else {
                                leaf.values().rev().nth(triplet.2)
                            };
                            match value_opt {
                                Some(value) => {
                                    triplet.2 += 1;
                                    break Some(((*value).clone(), triplet));
                                }
                                None => {
                                    triplet.1 = triplet.0.next().await;
                                    triplet.2 = 0;
                                    continue;
                                }
                            }
                        }
                        None => break None,
                    };
                }
            },
        ))
    }

    pub(crate) async fn stream_all_nodes(
        &self,
        direction: Direction,
    ) -> impl Stream<Item = Node<K, V>> + '_ {
        let seed = if direction == Direction::Ascending {
            1
        } else {
            self.index.load(Ordering::SeqCst)
        };
        self.inner_stream_nodes(seed, direction).await
    }

    async fn inner_stream_nodes(
        &self,
        seed_idx: usize,
        direction: Direction,
    ) -> impl Stream<Item = Node<K, V>> + '_ {
        let node_count = self.index.load(Ordering::SeqCst);
        Box::pin(stream::unfold(seed_idx, move |mut idx| async move {
            let mut nodes_lock = self.nodes.lock().await;
            match direction {
                Direction::Descending => {
                    // Needed to protect header record
                    while idx > 0 {
                        match self.find_node_with_lock(&mut nodes_lock, idx).await {
                            Ok(node) => {
                                return Some((node, idx - 1));
                            }
                            Err(_e) => {
                                idx -= 1;
                            }
                        }
                    }
                    None
                }
                Direction::Ascending => {
                    // Needed to protect header record
                    while idx > 0 && idx < node_count {
                        match self.find_node_with_lock(&mut nodes_lock, idx).await {
                            Ok(node) => {
                                return Some((node, idx + 1));
                            }
                            Err(_e) => {
                                idx += 1;
                            }
                        }
                    }
                    None
                }
            }
        }))
    }

    pub(crate) async fn stream_all_leaf_nodes(
        &self,
        direction: Direction,
    ) -> impl Stream<Item = Node<K, V>> + '_ {
        let seed = if direction == Direction::Ascending {
            self.first_leaf().await
        } else {
            self.last_leaf().await
        };

        self.inner_stream_leaf_nodes(seed, direction)
    }

    fn inner_stream_leaf_nodes(
        &self,
        seed: Node<K, V>,
        direction: Direction,
    ) -> impl Stream<Item = Node<K, V>> + '_ {
        Box::pin(stream::unfold(Some(seed), move |node_opt| async move {
            match node_opt {
                Some(node) => match self.neighbour(node.index(), direction).await {
                    Some(ns) => Some((node, Some(ns))),
                    None => Some((node, None)),
                },
                None => None,
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test_log::test(tokio::test)]
    async fn it_streams_leaf_nodes() {
        // Create test tree
        let tree = Baildon::<usize, usize>::try_new("streams_tree.db", 4)
            .await
            .expect("creates tree file");
        let input = vec![
            7, 8, 14, 20, 21, 27, 34, 42, 43, 47, 48, 52, 64, 72, 90, 91, 93, 94, 97,
        ];
        for i in &input {
            tree.insert(*i, *i).await.expect("insert worked");
        }

        // Test Ascending stream
        let mut streamer = tree.stream_all_leaf_nodes(Direction::Ascending).await;

        let mut slice_index = 0;
        while let Some(node) = streamer.next().await {
            let slice_len = node.len();
            let a = &node.keys().cloned().collect::<Vec<usize>>()[..];
            let b = &input[slice_index..slice_index + slice_len];
            assert_eq!(a, b);
            slice_index += slice_len;
        }

        // Test Descending stream
        let mut streamer = tree.stream_all_leaf_nodes(Direction::Descending).await;

        let mut slice_index = input.len();
        while let Some(node) = streamer.next().await {
            let slice_len = node.len();
            let a = &node.keys().cloned().collect::<Vec<usize>>()[..];
            let b = &input[slice_index - slice_len..slice_index];
            assert_eq!(a, b);
            slice_index -= slice_len;
        }

        // Delete test tree
        std::fs::remove_file("streams_tree.db").expect("cleanup");
    }
}
