use crate::{KleinDBError, SQLite3};

#[derive(Debug)]
enum InTransaction {
	None,
	Read,
	Write
}

#[derive(Debug)]
pub struct Btree {
	in_trans: InTransaction,
	// sharable: bool,
	// locked: bool,
}
pub struct BtreeShared {}
pub struct BtreeCursor {}
pub struct BtreePayload {}

impl Btree {
	pub fn new() -> Self {
		Self { in_trans: InTransaction::None }
	}

	/// Open a database file
	pub fn open(filename: &str, db: &SQLite3) -> Result<Self, KleinDBError> {
		let is_temp_db = false;
		let is_mem_db = if cfg!(feature = "omit_memorydb") {
			false
		} else {
			filename == ":memory:"
			// TODO:
			// (isTempDb && sqlite3TempInMemory(db) ||
			// (vfsFlags & SQLITE_OPEN_MEMORY)!=0
		};
		let btree = Btree::new();
		Ok(btree)
	}
}