//! This file contains the implementation of the sqlite3_prepare()
//! interface, and routines that contribute to loading the database schema
//! from disk.

use std::sync::{Arc, Mutex};

use chumsky::Parser;

use crate::{
  Cr, KleinDBContext, Parse, SQLite3, SQLite3Stmt,
  compiler::{codegen::generate_bytecode, parser::parser, tokenizer},
};

/// Compile the UTF-8 encoded SQL statement zSql into a statement handle.
fn sqlite3_prepare(db: &SQLite3, z_sql: &str) -> SQLite3Stmt {
  let tokens = tokenizer::tokenize(z_sql);

  let parse_ctx = Arc::new(Mutex::new(Parse {
    db,
    vdbe: SQLite3Stmt::new(),
    n_mem: 0,
    s_name_token: None,
    cr: Cr {
      addr_cr_tab: -1,
      reg_row_id: -1,
      reg_root: -1,
    },
  }));

  // Similar to sqlite3RunParser()
  let ast = parser(Arc::clone(&parse_ctx)).parse(&tokens).unwrap();
  println!("{:?}", ast);

  {
    let a_parse = Arc::clone(&parse_ctx);
    let mut p_parse = a_parse.lock().unwrap();
    generate_bytecode(&mut p_parse, ast);
  }

  // TODO: Final checks, error handling comes here

  let lock = Arc::into_inner(parse_ctx).unwrap();
  lock.into_inner().unwrap().vdbe
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
