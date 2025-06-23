//! This file contains the implementation of the sqlite3_prepare()
//! interface, and routines that contribute to loading the database schema
//! from disk.

use std::sync::Arc;

use crate::{KleinDBContext, Parse, SQLite3};

/// Compile the UTF-8 encoded SQL statement zSql into a statement handle.
fn sqlite3_prepare(db: &SQLite3, z_sql: &str) {
  let mut s_parse = Parse {};
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
