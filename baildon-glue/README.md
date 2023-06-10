# baildon-glue

A CLI which implements a simple SQL Database using GlueSQL

## Features

A simple SQL database CLI built using baildon and GlueSQL.

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

[![Crates.io](https://img.shields.io/crates/v/baildon-glue.svg)](https://crates.io/crates/baildon-glue)

## Installation

```sh
cargo install --bin baildon-glue
```

## License

Apache 2.0 licensed. See LICENSE for details.
