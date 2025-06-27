use std::sync::{Arc, Mutex};

pub mod compiler;

const SQLITE_DIGIT_SEPARATOR: u8 = b'_';

/// This context will be passed around throughout the lifetime
/// of a sql query
pub struct KleinDBContext {
  pub db: Arc<Mutex<SQLite3>>,
}

#[derive(Debug, PartialEq)]
pub enum TokenType {
  LeftParen,
  RightParen,
  Space,
  Comment,
  Ptr,
  Minus,
  Semi,
  Plus,
  Star,
  Slash,
  Rem,
  Eq,
  LE,
  NE,
  LShift,
  LT,
  GE,
  RShift,
  GT,
  Illegal,
  BitOr,
  Concat,
  Comma,
  BitAnd,
  BitNot,
  String,
  Id,
  Dot,
  Float,
  Integer,
  QNumber,
  Variable,
  Blob,

  // Keywords
  ABORT,
  ACTION,
  ADD,
  AFTER,
  ALL,
  ALTER,
  ALWAYS,
  ANALYZE,
  AND,
  AS,
  ASC,
  ATTACH,
  AUTOINCREMENT,
  BEFORE,
  BEGIN,
  BETWEEN,
  BY,
  CASCADE,
  CASE,
  CAST,
  CHECK,
  COLLATE,
  COLUMN,
  COMMIT,
  CONFLICT,
  CONSTRAINT,
  CREATE,
  CROSS,
  CURRENT,
  CURRENT_DATE,
  CURRENT_TIME,
  CURRENT_TIMESTAMP,
  DATABASE,
  DEFAULT,
  DEFERRABLE,
  DEFERRED,
  DELETE,
  DESC,
  DETACH,
  DISTINCT,
  DO,
  DROP,
  EACH,
  ELSE,
  END,
  ESCAPE,
  EXCEPT,
  EXCLUDE,
  EXCLUSIVE,
  EXISTS,
  EXPLAIN,
  FAIL,
  FILTER,
  FIRST,
  FOLLOWING,
  FOR,
  FOREIGN,
  FROM,
  FULL,
  GENERATED,
  GLOB,
  GROUP,
  GROUPS,
  HAVING,
  IF,
  IGNORE,
  IMMEDIATE,
  IN,
  INDEX,
  INDEXED,
  INITIALLY,
  INNER,
  INSERT,
  INSTEAD,
  INTERSECT,
  INTO,
  IS,
  ISNULL,
  JOIN,
  KEY,
  LAST,
  LEFT,
  LIKE,
  LIMIT,
  MATCH,
  MATERIALIZED,
  NATURAL,
  NO,
  NOT,
  NOTHING,
  NOTNULL,
  NULL,
  NULLS,
  OF,
  OFFSET,
  ON,
  OR,
  ORDER,
  OTHERS,
  OUTER,
  OVER,
  PARTITION,
  PLAN,
  PRAGMA,
  PRECEDING,
  PRIMARY,
  QUERY,
  RAISE,
  RANGE,
  RECURSIVE,
  REFERENCES,
  REGEXP,
  REINDEX,
  RELEASE,
  RENAME,
  REPLACE,
  RESTRICT,
  RETURNING,
  RIGHT,
  ROLLBACK,
  ROW,
  ROWS,
  SAVEPOINT,
  SELECT,
  SET,
  TABLE,
  TEMP,
  TEMPORARY,
  THEN,
  TIES,
  TO,
  TRANSACTION,
  TRIGGER,
  UNBOUNDED,
  UNION,
  UNIQUE,
}

#[derive(Debug, PartialEq)]
/// Each token coming out of the lexer is an instance of this structure
pub struct Token<'a> {
  pub text: &'a str,
  pub token_type: TokenType,
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

/// A single instruction of the virtual machine has an opcode
/// and as many as three operands.  The instruction is recorded
/// as an instance of the following structure:
pub struct VdbeOp {
  pub opcode: u8,
}

/// AKA Vdbe
pub struct SQLite3Stmt {
  /// The program counter
  pub pc: usize,

  /// Space to hold the virtual machine's program
  pub a_op: Vec<VdbeOp>,
}

/// OS Interface Object
/// Defines the interface between the SQLite core and the underlying operating system
pub struct SQLite3VFS {}

pub enum SQLite3ResultCodes {
  SQLiteOk,
  SQLiteError,
}

pub fn is_id_char(ch: u8) -> bool {
  // Identifiers are alphanumerics, "_", "$", and any non-ASCII UTF character.
  match ch {
    b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'$' | b'_' => true,
    128..=255 => true,
    _ => false,
  }
}
