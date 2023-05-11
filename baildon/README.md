## baildon

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
```

## Acknowledgements

TBD

## Benchmarks

If you want to look at my criterion generated [report](https://garypen.github.io/baildon/target/criterion/report/index.html), the clearest comparison is from clicking on the `get` link, but feel free to dig into the details.

If you want to generate your own set of benchmarking comparisons, download the repo and run the following:

```sh
cargo bench --bench baildon -- --plotting-backend gnuplot --baseline 0.1.0
```

This assumes that you have gnuplot installed on your system. (`apt install gnuplot`) and that you have installed [criterion](https://crates.io/crates/cargo-criterion) for benchmarking.

## License

Apache 2.0 licensed. See LICENSE for details.
