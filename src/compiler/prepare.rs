//! This file contains the implementation of the sqlite3_prepare()
//! interface, and routines that contribute to loading the database schema
//! from disk.

use std::sync::Arc;

use chumsky::Parser;

use crate::{
  KleinDBContext, Parse, SQLite3, SQLite3Stmt,
  compiler::{codegen::generate_bytecode, parser::parser},
};

/// Compile the UTF-8 encoded SQL statement zSql into a statement handle.
fn sqlite3_prepare(db: &SQLite3, z_sql: &str) -> SQLite3Stmt {
  let ast = parser().parse(z_sql).unwrap();
  println!("{:?}", ast);

  let bytecode = generate_bytecode(db, ast);

  SQLite3Stmt {
    pc: 0,
    a_op: bytecode,
  }
}

fn sqlite3_lock_and_prepare(ctx: &KleinDBContext, z_sql: &str) {
  let db = Arc::clone(&ctx.db);
  let db = db.lock().unwrap();

  // Do this until it succeeds or encounters a permanent error
  sqlite3_prepare(&db, z_sql);
}

pub fn sqlite3_prepare_v2(ctx: &KleinDBContext, z_sql: &str) {
  sqlite3_lock_and_prepare(ctx, z_sql);
}
