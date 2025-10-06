use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use bitflags::bitflags;

use crate::storage::btree::Btree;

pub mod compiler;
pub mod storage;

const SQLITE_DIGIT_SEPARATOR: u8 = b'_';

#[derive(Debug)]
pub enum StepStatus {
  Ok,
  Row,
  Done,
}

#[derive(Debug)]
pub enum KleinDBError {
  Error,
  Internal,
  Perm,
  Abort,
  Busy,
  Locked,
  NoMem,
  ReadOnly,
  Interrupt,
  IoErr,
  Corrupt,
  NotFound,
  Full,
  CantOpen,
  Protocol,
  Empty,
  Schema,
  TooBig,
  Constraint,
  Mismatch,
  Misuse,
  NoLfs,
  Auth,
  Format,
  Range,
  NotADb,
  Notice,
  Warning,
}

/// This context will be passed around throughout the lifetime
/// of a sql query
pub struct KleinDBContext {
  pub db: Arc<Mutex<SQLite3>>,
  pub vdbe: Option<SQLite3Stmt>,
}

impl KleinDBContext {
  pub fn sqlite3_step(&mut self) -> Result<StepStatus, ()> {
    // TODO: Enter db mutex
    // TODO: Retry if schema changes
    if let Some(vdbe) = &mut self.vdbe {
      Ok(vdbe.sqlite3_step())
    } else {
      Err(())
    }
  }

  pub fn set_vdbe(&mut self, p_stmt: SQLite3Stmt) {
    self.vdbe = Some(p_stmt);
  }

  // TODO
  fn sqlite3_parse_url() {}

  /// This routine does the work of opening a database on behalf of
  /// sqlite3_open() and sqlite3_open16(). The database filename "filename"
  /// is UTF-8 encoded.
  fn open_database(&mut self, filename: &str) -> Result<(), KleinDBError> {
    let db_handle = Arc::clone(&self.db);
    // Enter mutex
    let mut db = db_handle.lock().unwrap();

    // nDb = 2
    // db->aDb = db->aDbStatic;
    for _ in 0..2 {
      db.a_db.push(Db {
        db_schema_name: String::new(),
        bt: None,
        safety_level: 0,
        sync_set: false,
        schema: None,
      });
    }

    // TODO: Parse the filename/URI argument

    // Open the backend database driver
    db.a_db[0].bt = Some(Btree::open(filename, &db).unwrap());
    db.a_db[0].schema = Some(Self::sqlite3_schema_get(db.a_db[0].bt.as_mut()));
    Ok(())
  }

  /// Find and return the schema associated with a BTree.  Create
  /// a new one if necessary.
  fn sqlite3_schema_get(mut bt: Option<&mut Btree>) -> Arc<Schema> {
    let schema = if let Some(btr) = bt.as_mut() {
      btr.schema()
    } else {
      Arc::new(Schema {
        schema_cookie: 0,
        i_generation: 0,
      })
    };
    // TODO: Oom checn and file_format check
    schema
  }

  pub fn sqlite3_open_v2(&mut self, filename: &str) -> Result<(), KleinDBError> {
    self.open_database(filename)
  }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TokenType {
  // Dummy Token
  Dummy,

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

#[derive(Debug, PartialEq, Clone)]
/// Each token coming out of the lexer is an instance of this structure
pub struct Token<'a> {
  pub text: &'a str,
  pub token_type: TokenType,
}

impl<'a> Token<'a> {
  pub fn is_semi(&self) -> bool {
    self.token_type == TokenType::Semi
  }
}

#[derive(Debug)]
struct Cr {
  addr_cr_tab: i32,
  reg_row_id: i32,
  reg_root: i32,
}

// #[derive(Debug)]
// struct D {}

/// An SQL parser context. A copy of this structure is passed through
/// the parser and down into all the parser action routine in order to
/// carry around information that is global to the entire parse.
///
/// The structure is divided into two parts.  When the parser and code
/// generate call themselves recursively, the first part of the structure
/// is constant but the second part is reset at the beginning and end of
/// each recursion.
#[derive(Debug)]
pub struct Parse<'a> {
  /// The main database structure
  db: &'a SQLite3,
  /// An engine for executing database bytecode
  vdbe: SQLite3Stmt,
  /// Number of memory cells used so far
  n_mem: usize,

  /// unqualified schema object name
  s_name_token: Option<Token<'a>>,

  /// These fields available when isCreate is true
  cr: Cr,
}

impl<'a> Parse<'a> {
  /// The table or view or trigger name is passed to this routine via tokens
  /// pName1 and pName2. If the table name was fully qualified, for example:
  ///
  /// CREATE TABLE xxx.yyy (...);
  ///
  /// Then p_name1 is set to "xxx" and p_name2 "yyy". On the other hand if
  /// the table name is not fully qualified, i.e.:
  ///
  /// CREATE TABLE yyy(...);
  ///
  /// Then p_name1 is set to "yyy" and p_name2 is "".
  ///
  /// This routine returns a tuple (&Token, usize) where Token is
  /// the un-qualified pointer to either p_name1 or p_name2. usize
  /// is the index of database xxx
  pub fn sqlite3_two_part_name(
    &self,
    p_name1: &'a Token,
    p_name2: &'a Token,
  ) -> Result<(&'a Token<'a>, usize), ()> {
    let db = self.db;
    if p_name2.text.len() > 0 {
      // Same as token_type == Dummy
      if db.init.busy {
        // corrupt database
        return Err(());
      }

      let i_db = sqlite3_find_db(db, p_name1);

      if let Some(i) = i_db {
        Ok((p_name2, i))
      } else {
        Err(())
      }
    } else {
      // TODO: Look at this assert more closely
      //assert( db->init.iDb==0 || db->init.busy || IN_SPECIAL_PARSE
      //  || (db->mDbFlags & DBFLAG_Vacuum)!=0);
      Ok((p_name1, db.init.i_db.into()))
    }
  }
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

/// Each database file to be accessed by the system is an instance
/// of the following structure.  There are normally two of these structures
/// in the sqlite.aDb[] array.  aDb[0] is the main database file and
/// aDb[1] is the database file used to hold temporary tables.  Additional
/// databases may be attached.
#[derive(Debug)]
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

type Pgno = i32;

/// Information used during initialization
#[derive(Debug)]
pub struct SQLite3InitInfo {
  /// Rootpage of table being initialized
  pub new_t_num: Pgno,
  /// Which db file is being initialized
  pub i_db: u8,
  pub busy: bool,
}

/// Each database connection is an instance of the following structure.
#[derive(Debug)]
pub struct SQLite3 {
  /// All backends
  pub a_db: Vec<Db>,
  pub init: SQLite3InitInfo,
}

impl SQLite3 {
  pub fn new() -> Self {
    Self {
      a_db: vec![],
      init: SQLite3InitInfo {
        new_t_num: 0,
        i_db: 0,
        busy: false,
      },
    }
  }

  pub fn sqlite3_find_db_name(&self, z_name: &str) -> Option<usize> {
    let i = self.a_db.iter().position(|db| db.db_schema_name == z_name);
    if let Some(idx) = i {
      Some(idx)
    } else {
      if self.a_db.iter().position(|db| db.db_schema_name == "main") == Some(0) {
        Some(0)
      } else {
        None
      }
    }
  }
}

#[derive(Debug)]
pub enum Opcode {
  Init,
  Integer,
  ResultRow,
  Halt,
  Goto,
  ReadCookie,
  If,
  SetCookie,
  NewRowid,
  Blob,
  Insert,
  Close,
  CreateBtree,
}

pub enum TextEncodings {
  Utf8 = 1,
  Utf16LE,
  Utf16BE,
  Utf16,
  Any,
  Utf16Aligned = 8,
}

#[derive(Debug)]
enum P4Union {
  Strings(String),
  Int32(i32),
  Int64(i64),
  Real(f64),
}

#[derive(Debug, Clone)]
pub enum P4Type {
  Transient,
  Static = -1,
  CollSeq = -2,
  Int32 = -3,
  SubProgram = -4,
  Table = -5,
  Dynamic = -6,
}

bitflags! {
  #[derive(Debug)]
  pub struct OpFlags: u32 {
    const APPEND = 0x08;
  }
}

/// A single instruction of the virtual machine has an opcode
/// and as many as three operands.  The instruction is recorded
/// as an instance of the following structure:
#[derive(Debug)]
pub struct VdbeOp {
  pub opcode: Opcode,
  pub p1: i32,
  pub p2: i32,
  pub p3: i32,
  pub p4type: Option<P4Type>,
  pub p4: Option<P4Union>,
  pub p5: Option<OpFlags>,
}

#[derive(Debug, Clone)]
pub enum MemValue {
  Undefined,
  Integer(i32),
  Real(f64),
}

/// These are Mems
#[derive(Debug, Clone)]
pub struct SQLite3Value {
  pub value: MemValue,
}

/// AKA VDBE
#[derive(Debug)]
pub struct SQLite3Stmt {
  /// The program counter
  pub pc: usize,

  /// Space to hold the virtual machine's program
  pub a_op: Vec<VdbeOp>,

  /// The memory locations
  pub a_mem: Vec<SQLite3Value>,

  /// Index in a_mem to start reading result from
  pub result_row: usize,

  pub n_res_column: usize,
}

impl SQLite3Stmt {
  // Create a new virtual database engine.
  pub fn new() -> Self {
    let mut stmt = Self {
      pc: 0,
      a_op: vec![],
      // Not sure how many or what determines the number so I
      // am making an initial guess till I figure it out
      a_mem: vec![
        SQLite3Value {
          value: MemValue::Undefined
        };
        20
      ],
      result_row: 0,
      n_res_column: 0,
    };
    stmt.sqlite3_add_op2(Opcode::Init, 0, 1);
    stmt
  }

  pub fn sqlite3_add_op0(&mut self, op: Opcode) -> usize {
    self.sqlite3_add_op3(op, 0, 0, 0)
  }

  pub fn sqlite3_add_op1(&mut self, op: Opcode, p1: i32) -> usize {
    self.sqlite3_add_op3(op, p1, 0, 0)
  }

  pub fn sqlite3_add_op2(&mut self, op: Opcode, p1: i32, p2: i32) -> usize {
    self.sqlite3_add_op3(op, p1, p2, 0)
  }

  pub fn sqlite3_add_op3(&mut self, op: Opcode, p1: i32, p2: i32, p3: i32) -> usize {
    let i = self.a_op.len();
    self.a_op.push(VdbeOp {
      opcode: op,
      p1,
      p2,
      p3,
      p4type: None,
      p4: None,
      p5: None,
    });
    i
  }

  pub fn sqlite3_vdbe_get_op(&mut self, addr: usize) -> &mut VdbeOp {
    &mut self.a_op[addr]
  }

  pub fn sqlite3_vdbe_change_p2(&mut self, addr: usize, val: i32) {
    self.sqlite3_vdbe_get_op(addr).p2 = val;
  }

  /// Change the P2 operand of instruction addr so that it points to
  /// the address of the next instruction to be coded
  pub fn sqlite3_vdbe_jump_here(&mut self, addr: usize) {
    self.sqlite3_vdbe_change_p2(addr, self.a_op.len() as i32)
  }

  /// Generate code for an unconditional jump to instruction iDest
  pub fn sqlite3_vdbe_goto(&mut self, i_dest: i32) -> usize {
    self.sqlite3_add_op3(Opcode::Goto, 0, i_dest, 0)
  }

  /// Add an opcode that includes the p4 value as a pointer.
  pub fn sqlite3_add_op4(
    &mut self,
    op: Opcode,
    p1: i32,
    p2: i32,
    p3: i32,
    p4: String,
    p4type: P4Type,
  ) {
    let addr = self.sqlite3_add_op3(op, p1, p2, p3);
    self.sqlite3_vdbe_change_p4(addr as i32, p4, p4type);
  }

  /// Change the value of the P4 operand for a specific instruction.
  pub fn sqlite3_vdbe_change_p4(&mut self, addr: i32, p4: String, n: P4Type) {
    let mut addr = addr as usize;
    if addr < 0 {
      addr = self.a_op.len() - 1;
    }
    let op = &self.a_op[addr];
    if n.clone() as i32 >= 0 || op.p4type.is_some() {
      self.vdbe_change_p4_full(addr, p4, n);
    }
    // TODO: Handle P4_INT32
  }

  /// Change the value of the P4 operand for a specific instruction.
  pub fn vdbe_change_p4_full(&mut self, addr: usize, p4: String, n: P4Type) {
    if (n.clone() as i32) < 0 {
      self.sqlite3_vdbe_change_p4(addr as i32, p4, n);
    } else {
      // From the original source, if n is 0, the length of
      // p4 is calculated and n is updated, otherwise n is the
      // length of the string
      self.a_op[addr].p4 = Some(P4Union::Strings(p4));
      self.a_op[addr].p4type = Some(P4Type::Dynamic);
    }
  }

  pub fn sqlite3_vdbe_change_p5(&mut self, p5: OpFlags) {
    let n = self.a_op.len();
    if n > 0 {
      self.a_op[n - 1].p5 = Some(p5);
    }
  }

  pub fn sqlite3_column_count(&self) -> usize {
    self.n_res_column
  }

  pub fn sqlite3_column_text(&self, i: usize) -> String {
    match self.a_mem[self.result_row + i].value {
      MemValue::Undefined => String::from("undefined"),
      MemValue::Integer(x) => x.to_string(),
      MemValue::Real(x) => x.to_string(),
    }
  }

  /// Execute as much of a VDBE program as we can.
  /// This is the core of sqlite3_step().
  fn sqlite3_vdbe_exec(&mut self) -> StepStatus {
    let n = self.a_op.len() as i32;
    let mut step_pc: usize = self.pc;

    let mut status = StepStatus::Ok;
    loop {
      let p_op = &mut self.a_op[step_pc];
      match p_op.opcode {
        Opcode::Init => {
          p_op.p1 += 1;
          // Jump to p2
          assert!(p_op.p2 > 0);
          assert!(p_op.p2 < n);
          step_pc = p_op.p2 as usize;
        }
        Opcode::Integer => {
          self.a_mem[p_op.p2 as usize].value = MemValue::Integer(p_op.p1);
          step_pc += 1;
        }
        Opcode::ResultRow => {
          // TODO: bound checks

          self.result_row = p_op.p1 as usize;
          self.pc = step_pc + 1;
          status = StepStatus::Row;
          break;
        }
        Opcode::Halt => {
          // TODO: Implement sqlite3VdbeHalt()
          status = StepStatus::Done;
          break;
        }
        Opcode::Goto => {
          step_pc = p_op.p2 as usize;
        },
        _ => {}
      }
    }

    // vdbe_return:
    status
  }

  /// Same as sqlite3Step
  /// Execute the statement pStmt, either until a row of data is ready, the
  /// statement is completely executed or an error occurs.
  fn sqlite3_step(&mut self) -> StepStatus {
    self.sqlite3_vdbe_exec()
  }
}

/// OS Interface Object
/// Defines the interface between the SQLite core and the underlying operating system
pub struct SQLite3VFS {}

pub enum SQLite3ResultCodes {
  SQLiteOk,
  SQLiteError,
}

pub enum SelectResultType {
  Union = 1,
  Except,
  Exists,
  Discard,
  DistFifo,
  DistQueue,

  Queue,
  Fifo,

  /// Output each row of result
  Output,
  Mem,
  Set,
  EphemTab,
  Coroutine,
  Table,
  Upfrom,
}

/// An instance of this object describes where to put of the results of
/// a SELECT statement.
pub struct SelectDest {
  pub e_dest: SelectResultType,
  /// Base register where results are written
  pub i_sdst: usize,
  /// Number of registers allocated
  pub n_sdst: usize,
}

pub fn is_id_char(ch: u8) -> bool {
  // Identifiers are alphanumerics, "_", "$", and any non-ASCII UTF character.
  match ch {
    b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'$' | b'_' => true,
    128..=255 => true,
    _ => false,
  }
}

pub fn sqlite3_find_db(db: &SQLite3, p_name: &Token) -> Option<usize> {
  let z_name = sqlite3_name_from_token(db, p_name);
  db.sqlite3_find_db_name(z_name)
}

pub fn sqlite3_name_from_token<'a>(db: &SQLite3, p_name: &'a Token) -> &'a str {
  sqlite3_dequote(p_name.text)
}

pub fn sqlite3_dequote<'a>(z_name: &'a str) -> &'a str {
  let mut chrs = z_name.chars();
  let mut q: Option<char> = chrs.nth(0);

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
