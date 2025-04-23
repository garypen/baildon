#![warn(missing_docs)]
//! Baildon B+Tree
//!
//! # Why is this called Baildon?
//!
//! All the good names for B+Trees have gone, so I decided to just name the crate after the town
//! where I live.
//!
//! # Implementation Details
//!
//! Provides a simple B+Tree implementation for storing keys and values in a file.
//!
//! The implementation stores keys and values in a B+Tree with a user specified branching
//! factor.
//!
//! (If you aren't sure what that means, you can read more about B+Trees here: <https://en.wikipedia.org/wiki/B%2B_tree_>.)
//!
//! The B+Tree is composed of nodes, which are all stored in an in-memory cache. Each node
//! has a unique index and the index is used to reference the nodes from the set of known nodes.
//!
//! If a node isn't present in the in-memory cache, it is loaded in from backing storage on
//! demand.
//!
//! A node is either:
//!  - Leaf node, contains keys and values
//!  - Internal node, contains keys and Leaf node indices
//!
//! At load/store to disk, a node is serialized/deserialized using bincode.
//!
//! When a reference to a B+Tree is dropped, dirty data is flushed to disk. Alternatively, a flush
//! can also be manually triggered if desired.
//!
//! If the process fails in any way before data is safely on disk, then the Write Ahead Log (WAL)
//! will be used when the B+Tree is next opened to recover lost modifications.
//!
//! Note: Most of these details (nodes, file access, serialization format) are tucked away inside
//! the implementation.  The user experience should be similar to working with a BTreeMap, but
//! slower when I/O is involved.
//!

pub mod btree;
mod command;
mod io;

use bincode::config::AllowTrailing;
use bincode::config::FixintEncoding;
use bincode::config::WithOtherIntEncoding;
use bincode::config::WithOtherTrailing;
use bincode::{DefaultOptions, Options};
use std::sync::LazyLock;

static BINCODER: LazyLock<
    WithOtherIntEncoding<WithOtherTrailing<DefaultOptions, AllowTrailing>, FixintEncoding>,
> = LazyLock::new(|| {
    bincode::DefaultOptions::new()
        .allow_trailing_bytes()
        .with_fixint_encoding()
});
