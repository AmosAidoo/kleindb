//! This file contains the implementation of the sqlite3_prepare()
//! interface, and routines that contribute to loading the database schema
//! from disk.

use std::sync::Arc;

use chumsky::Parser;

use crate::{
  KleinDBContext, Parse, SQLite3, SQLite3Stmt,
  compiler::{codegen::generate_bytecode, parser::parser, tokenizer},
};

/// Compile the UTF-8 encoded SQL statement zSql into a statement handle.
fn sqlite3_prepare(db: &SQLite3, z_sql: &str) -> SQLite3Stmt {
  let tokens = tokenizer::tokenize(z_sql);

  // Similar to sqlite3RunParser()
  let ast = parser().parse(&tokens).unwrap();
  println!("{:?}", ast);

  let mut p_parse = Parse {
    db,
    vdbe: SQLite3Stmt::new(),
    n_mem: 0,
  };
  generate_bytecode(&mut p_parse, ast);

  // TODO: Final checks, error handling comes here

  p_parse.vdbe
}

fn sqlite3_lock_and_prepare(ctx: &KleinDBContext, z_sql: &str) -> SQLite3Stmt {
  let db = Arc::clone(&ctx.db);
  let db = db.lock().unwrap();

  // Do this until it succeeds or encounters a permanent error
  sqlite3_prepare(&db, z_sql)
}

pub fn sqlite3_prepare_v2(ctx: &KleinDBContext, z_sql: &str) -> SQLite3Stmt {
  sqlite3_lock_and_prepare(ctx, z_sql)
}
