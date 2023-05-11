# baildon-glue

A CLI which implements a simple SQL Database using GlueSQL

> **_Note:_** Not released to crates.io as yet because the implementation is against `main` branch of GlueSQL.

## Features

A simple SQL database CLI which supports all the SQL features provided by GlueSQL.

```sh
baildon-glue --help
Simple SQL Database

Usage: baildon-glue [OPTIONS] <DATABASE>

Arguments:
  <DATABASE>  Database location

Options:
  -c, --create   Create a new database (will overwrite existing file)
  -h, --help     Print help
  -V, --version  Print version
```

[![Crates.io](https://img.shields.io/crates/v/baildon.svg)](https://crates.io/crates/baildon)

[API Docs](https://docs.rs/baildon/latest/baildon)

## Installation

```sh
cargo install --bin baildon-glue
```

## License

Apache 2.0 licensed. See LICENSE for details.
