use std::sync::{Arc, Mutex};

use chumsky::Parser;

use crate::{
  compiler::{
    codegen::generate_bytecode,
    parser::parser,
    tokenizer::{self, token::Token},
  },
  core::parse::{Cr, Parse},
  storage::btree::Btree,
  vdbe::stmt::SQLite3Stmt,
};

/// Each database connection is an instance of the following structure.
#[derive(Debug, Default)]
pub struct SQLite3 {
  /// All backends
  pub a_db: Vec<Db>,
  pub init: SQLite3InitInfo,
}

impl SQLite3 {
  pub fn sqlite3_find_db_name(&self, z_name: &str) -> Option<usize> {
    // Prefer the exact match; otherwise fall back to index 0 if it is the
    // "main" database. Use iterator adaptors for clarity.
    self
      .a_db
      .iter()
      .position(|db| db.db_schema_name == z_name)
      .or_else(|| {
        self
          .a_db
          .first()
          .filter(|db| db.db_schema_name == "main")
          .map(|_| 0usize)
      })
  }

  pub fn sqlite3_find_db(&self, p_name: &Token) -> Option<usize> {
    let z_name = sqlite3_name_from_token(p_name);
    self.sqlite3_find_db_name(z_name)
  }

  /// Compile the UTF-8 encoded SQL statement zSql into a statement handle.
  pub(crate) fn sqlite3_prepare(&self, z_sql: &str) -> SQLite3Stmt {
    let tokens = tokenizer::token::tokenize(z_sql);
    println!("{:?}", tokens);

    let parse_ctx = Arc::new(Mutex::new(Parse {
      db: self,
      vdbe: SQLite3Stmt::new(),
      n_mem: 0,
      s_name_token: None,
      const_expr: None,
      ok_const_factor: true,
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
}

/// Each database file to be accessed by the system is an instance
/// of the following structure.  There are normally two of these structures
/// in the sqlite.aDb[] array.  aDb[0] is the main database file and
/// aDb[1] is the database file used to hold temporary tables.  Additional
/// databases may be attached.
#[derive(Debug, Default)]
pub struct Db {
  pub db_schema_name: String,
  /// The BTree structure for this database file
  pub bt: Option<Btree>,
  /// How aggressive at syncing data to disk
  pub safety_level: u8,
  /// True if "PRAGMA synchronous=N" has been run
  pub sync_set: bool,
  /// Pointer to database schema (possibly shared)
  pub schema: Option<Arc<Schema>>,
}

/// An instance of the following structure stores a database schema.
#[derive(Debug, PartialEq)]
pub struct Schema {
  /// Database schema version number for this file
  pub schema_cookie: i32,
  /// Generation counter.  Incremented with each change
  pub i_generation: i32,
  // All tables indexed by name
  // tbl_hash: HashMap<>
}

type Pgno = i32;

/// Information used during initialization
#[derive(Debug, Default)]
pub struct SQLite3InitInfo {
  /// Rootpage of table being initialized
  pub new_t_num: Pgno,
  /// Which db file is being initialized
  pub i_db: u8,
  pub busy: bool,
}

fn sqlite3_dequote(z_name: &str) -> &str {
  let mut chrs = z_name.chars();
  let mut q: Option<char> = chrs.next();

  if let Some(quote) = q.as_mut() {
    if matches!(quote, '\'' | '[' | '"' | '`') {
      *quote = ']';

      let mut i: usize = 1;

      while i < z_name.len() {
        if chrs.nth(i) == Some(*quote) {
          if i + 1 < z_name.len() && chrs.nth(i + 1) == Some(*quote) {
            i += 1;
          } else {
            break;
          }
        }
        i += 1;
      }

      &z_name[1..i]
    } else {
      z_name
    }
  } else {
    z_name
  }
}

pub fn sqlite3_name_from_token<'a>(p_name: &'a Token) -> &'a str {
  sqlite3_dequote(p_name.text)
}
