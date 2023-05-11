# baildon
asynchronous B+Tree

There are three components:
 - [baildon](baildon/README.md): a library which implements a simple B+Tree
 - [baildon-store](baildon-store/README.md): a CLI which implements a Key/Value store
 - [baildon-glue](baildon-glue/README.md): a CLI which implements GlueSQL to provide a simple database using baildon

baildon is the main deliverable from this repo, the two CLIs mainly exist to demonstrate how to use the library.

> **_Note:_** baildon-glue is not released to crates.io as yet because the implementation is against `main` branch of GlueSQL.

