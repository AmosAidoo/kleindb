use thiserror::Error;

#[derive(Debug, Error)]
pub enum KleinDBError {
  #[error("Generic error")]
  Error,
  #[error("Internal error")]
  Internal,
  #[error("Permission denied")]
  Perm,
  #[error("Operation aborted")]
  Abort,
  #[error("Database is busy")]
  Busy,
  #[error("Database is locked")]
  Locked,
  #[error("Out of memory")]
  NoMem,
  #[error("Database is read-only")]
  ReadOnly,
  #[error("Operation interrupted")]
  Interrupt,
  #[error("I/O error")]
  IoErr,
  #[error("Database is corrupt")]
  Corrupt,
  #[error("Not found")]
  NotFound,
  #[error("Database is full")]
  Full,
  #[error("Cannot open database")]
  CantOpen,
  #[error("Protocol error")]
  Protocol,
  #[error("Empty result")]
  Empty,
  #[error("Schema error")]
  Schema,
  #[error("Data too big")]
  TooBig,
  #[error("Constraint violation")]
  Constraint,
  #[error("Type mismatch")]
  Mismatch,
  #[error("Library misuse")]
  Misuse,
  #[error("No LFS support")]
  NoLfs,
  #[error("Authorization denied")]
  Auth,
  #[error("Format error")]
  Format,
  #[error("Value out of range")]
  Range,
  #[error("Not a database file")]
  NotADb,
  #[error("Notice")]
  Notice,
  #[error("Warning")]
  Warning,

  // Additional variants for better error handling
  #[error("No database connection")]
  NoDatabase,
  #[error("Invalid column index: {0}")]
  InvalidColumn(usize),
  #[error("Type conversion error: expected {expected}, got '{got}'")]
  TypeConversion { expected: String, got: String },
  #[error("I/O error: {0}")]
  Io(#[from] std::io::Error),
  #[error("SQL error: {0}")]
  Sql(String),
}
