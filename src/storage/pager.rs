use std::{fs::File, sync::Arc};

use crate::KleinDBError;

#[derive(Debug)]
pub struct Pager {
  pcache: Option<Arc<PCache>>,
}

#[derive(Debug)]
pub struct PCache {}

const SQLITE_DEFAULT_PAGE_SIZE: usize = 4096;

impl Pager {
  /// Allocate and initialize a new Pager object
  pub fn open(filename: &str) -> Result<Self, KleinDBError> {
    // TODO: Figure out how much space is required for each journal file-handle
    // (there are two of them, the main journal and the sub-journal).

    // TODO: Compute and store the full pathname in an allocated buffer pointed
    // to by filename

    // Allocate memory for the Pager structure
    let mut pager = Pager { pcache: None };

    // Open the pager file
    let f = File::open(filename).unwrap();
    // let readonly = false;

    // TODO: If the file was successfully opened for read/write access,
    // choose a default page size in case we have to create the
    // database file. The default page size is the maximum of:
    // SQLITE_DEFAULT_PAGE_SIZE,
    // sqlite3OsSectorSize()
    // The largest page size that can be written atomically

    // Initialize the PCache object
    pager.pcache = Some(Arc::new(PCache::open().unwrap()));

    Ok(pager)
  }
}

impl PCache {
  fn open() -> Result<Self, KleinDBError> {
    Ok(Self {})
  }
}
