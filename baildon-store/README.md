# baildon-store

A CLI which implements a Key/Value store using `baildon`.

## Features

A simple K/V store CLI which supports the usual CRUD operations for String Key and Value.

```sh
baildon-store --help
B+Tree CLI

Usage: baildon-store [OPTIONS] <STORE> [COMMAND]

Commands:
  contains  Does our store contain this key
  clear     Clear store entries
  count     Display B+Tree entry count
  delete    Delete this key
  entries   List store entries
  get       Get this key
  help      Interactive Help
  insert    Insert key value pair
  keys      List store keys
  nodes     List store nodes
  values    List store values
  verify    Verify store

Arguments:
  <STORE>  Store location

Options:
  -c, --create   Create a new store (will overwrite existing file)
  -h, --help     Print help
  -V, --version  Print version
```

[![Crates.io](https://img.shields.io/crates/v/baildon.svg)](https://crates.io/crates/baildon)

[API Docs](https://docs.rs/baildon/latest/baildon)

## Installation

```sh
cargo install --bin baildon-store
```

## License

Apache 2.0 licensed. See LICENSE for details.
