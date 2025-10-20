# kleindb

kleindb is a learning project. I am learning about database internals and have decided to read SQLite's source code and understand how it works. I am doing so by writing the code I read in Rust. This method makes me think a lot and absorb the code I am reading as well as understand the concepts well.

## Why Rust though

I am learning Rust so this choice was so I get better at it.

## Approach

This project contains a single binary crate and a library crate. The binary crate is an interactive shell for typing SQL statements and getting responses. The shell program that ships with SQLite was actually my entry point into the codebase. I figured that would be nicer since I can look at how it uses the public SQLite apis to execute the statements and dive deeper.

## Technologies
- Rust
- [chumsky](https://github.com/zesterer/chumsky) - a parser combinator library

## Other sections: TODO