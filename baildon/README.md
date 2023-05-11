## baildon

A very simple B+Tree library.

Features:

 - Generic B+Tree
 - Asynchronous (uses tokio)
 - Write Ahead Log
 - serde based storage format (bincode)

```rust
use baildon::tree::Baildon;
use baildon::tree::Direction;

// Create a B+Tree with usize for key and value, branching factor 7
let tree = Baildon::<usize, usize>::try_new("retrieve_keys_from_empty_tree.db", 7)
    .await
    .expect("creates tree file");

// Collect all our keys
let keys = tree
    .keys(Direction::Ascending)
    .await
    .collect::<Vec<usize>>()
    .await;

// It should be empty, we didn't add any keys
assert!(keys.is_empty());

// Remove our B+Tree file, we aren't going to use it again
std::fs::remove_file("retrieve_keys_from_empty_tree.db").expect("cleanup");
```

[![Crates.io](https://img.shields.io/crates/v/baildon.svg)](https://crates.io/crates/baildon)

[API Docs](https://docs.rs/baildon/latest/baildon)

## Installation

```toml
[dependencies]
baildon = "0.1"
```

## Examples

There are a few simple examples to show how to use the library:

```sh
cargo run --example hello
cargo run --example streaming
```

## Benchmarks

I've got some very simple benchmarks that I've used during development to look for regressions. I'll aim to improve these at some point.

```sh
cargo bench --bench baildon
```

## License

Apache 2.0 licensed. See LICENSE for details.
