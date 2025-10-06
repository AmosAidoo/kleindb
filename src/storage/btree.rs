use std::{
  ops::{Deref, DerefMut},
  sync::Arc,
};

use crate::{KleinDBError, SQLite3, Schema, storage::pager::Pager};

#[derive(Debug)]
enum InTransaction {
  None,
  Read,
  Write,
}

#[derive(Debug)]
pub struct Btree {
  in_trans: InTransaction,
  // sharable: bool,
  // locked: bool,
  /// Sharable content of this btree
  bt_shared: Option<Arc<BtShared>>,
}

#[derive(Debug)]
pub struct BtShared {
  /// Pointer to space allocated by sqlite3BtreeSchema()
  /// In this case Btree::schema()
  schema: Option<Arc<Schema>>,
  /// The page cache
  pager: Option<Arc<Pager>>,
}

pub enum BtreeConstants {
  FreePageCount,
  SchemaVersion,
  FileFormat,
  DefaultCacheSize,
  LargestRootPage,
  TextEncoding,
  UserVersion,
  IncrVacuum,
  ApplicationId,
  DataVersion = 15,
}

pub enum BtreeCreateTableFlags {
  IntKey = 1,
  BlobKey,
}

pub struct BtCursor {}
pub struct BtreePayload {}

impl Btree {
  pub fn new() -> Self {
    Self {
      in_trans: InTransaction::None,
      bt_shared: None,
    }
  }

  /// Open a database file
  pub fn open(filename: &str, db: &SQLite3) -> Result<Self, KleinDBError> {
    let is_temp_db = false;
    let is_mem_db = if cfg!(feature = "omit_memorydb") {
      false
    } else {
      filename == ":memory:"
      // TODO:
      // (isTempDb && sqlite3TempInMemory(db)) ||
      // (vfsFlags & SQLITE_OPEN_MEMORY)!=0
    };
    let btree = Btree::new();

    // Currently in happy path so this is probably
    // always true
    if btree.bt_shared.is_none() {
      let mut shared = BtShared {
        schema: None,
        pager: None,
      };
      shared.pager = Some(Arc::new(Pager::open(filename).unwrap()))
    }

    Ok(btree)
  }

  pub fn schema(&mut self) -> Arc<Schema> {
    // TODO: enter btree mutex
    let btshrd = self.bt_shared.as_mut().unwrap();
    // TODO: Not 100 percent sure about this.
    let shrd = Arc::get_mut(btshrd).unwrap();
    if shrd.schema.is_none() {
      shrd.schema = Some(Arc::new(Schema {
        schema_cookie: 0,
        i_generation: 0,
      }))
    }

    Arc::clone(btshrd.schema.as_ref().unwrap())
  }
}
