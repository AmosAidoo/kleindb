use std::sync::{Arc, Mutex};

pub mod compiler;

/// This context will be passed around throughout the lifetime
/// of a sql query
pub struct KleinDBContext {
    db: Arc<Mutex<SQLite3>>,
}

/// An SQL parser context. A copy of this structure is passed through
/// the parser and down into all the parser action routine in order to
/// carry around information that is global to the entire parse.
///
/// The structure is divided into two parts.  When the parser and code
/// generate call themselves recursively, the first part of the structure
/// is constant but the second part is reset at the beginning and end of
/// each recursion.
pub struct Parse {}

/// Each database connection is an instance of the following structure.
pub struct SQLite3 {}

pub struct SQLite3Stmt {}

/// OS Interface Object
/// Defines the interface between the SQLite core and the underlying operating system
pub struct SQLite3VFS {}

pub enum SQLite3ResultCodes {
    SQLiteOk,
    SQLiteError,
}
