# baildon
asynchronous B+Tree

There are three components:
 - baildon: a library which implements a simple B+Tree
 - baildon-store: a CLI which implements a Key/Value store
 - baildon-glue: a CLI which implemnts GlueSQL to provide a simple database using baildon

baildon is the main deliverable from this repo, the two CLIs mainly exist to demonstrate how to use the library.

> **_Note:_** baildon-glue is not released to crates.io as yet because the implementation is against `main` branch of GlueSQL.

