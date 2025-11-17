use bitflags::bitflags;

/// A single instruction of the virtual machine has an opcode
/// and as many as three operands.  The instruction is recorded
/// as an instance of the following structure:
#[derive(Debug, Clone)]
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
pub enum P4Union {
  String(String),
  Int32(i32),
  Int64(i64),
  Real(f64),
}

#[derive(Debug, Clone)]
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
  OpenWrite,
  Add,
  Subtract,
  Remainder,
  Divide,
  Multiply,
  And,
  Or,
  BitAnd,
  BitOr,
  ShiftLeft,
  ShiftRight,
  Concat,
  String,
  String8,
  Eq,
  ZeroOrNull,
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
  #[derive(Debug, Clone)]
  pub struct OpFlags: u32 {
    const APPEND = 0x08;
    const ISNOOP = 0x40;
    const LENGTHARG = 0x40;
  }
}
