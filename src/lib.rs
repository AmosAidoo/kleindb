use std::sync::{Arc, Mutex};

pub mod compiler;

const SQLITE_DIGIT_SEPARATOR: u8 = b'_';

#[derive(Debug)]
pub enum StepStatus {
  Ok,
  Row,
  Done,
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
}

impl KleinDBContext {
  pub fn set_vdbe(&mut self, p_stmt: SQLite3Stmt) {
    self.vdbe = Some(p_stmt);
  }
}

#[derive(Debug, PartialEq, Clone)]
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

/// An SQL parser context. A copy of this structure is passed through
/// the parser and down into all the parser action routine in order to
/// carry around information that is global to the entire parse.
///
/// The structure is divided into two parts.  When the parser and code
/// generate call themselves recursively, the first part of the structure
/// is constant but the second part is reset at the beginning and end of
/// each recursion.
pub struct Parse<'a> {
  /// The main database structure
  db: &'a SQLite3,
  /// An engine for executing database bytecode
  vdbe: SQLite3Stmt,
  /// Number of memory cells used so far
  n_mem: usize,
}

/// Each database connection is an instance of the following structure.
pub struct SQLite3 {}

#[derive(Debug)]
pub enum Opcode {
  Init,
  Integer,
  ResultRow,
  Halt,
  Goto,
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
}

#[derive(Clone)]
pub enum MemValue {
  Undefined,
  Integer(i32),
  Real(f64),
}

/// These are Mems
#[derive(Clone)]
pub struct SQLite3Value {
  pub value: MemValue,
}

/// AKA VDBE
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
        }
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
