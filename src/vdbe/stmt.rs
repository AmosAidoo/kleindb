use crate::{
  core::result::StepStatus,
  vdbe::{
    mem::{MemValue, SQLite3Value},
    op::{OpFlags, Opcode, P4Type, P4Union, VdbeOp},
  },
};

#[derive(Debug, Clone)]
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

impl Default for SQLite3Stmt {
  fn default() -> Self {
    Self::new()
  }
}

impl SQLite3Stmt {
  // Create a new virtual database engine.
  pub fn new() -> Self {
    let mut stmt = Self {
      pc: 0,
      a_op: vec![],
      a_mem: vec![],
      result_row: 0,
      n_res_column: 0,
    };
    stmt.sqlite3_add_op2(Opcode::Init, 0, 1);
    stmt
  }

  pub fn current_addr(&self) -> usize {
    self.a_op.len()
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
  ) -> i32 {
    let addr = self.sqlite3_add_op3(op, p1, p2, p3) as i32;
    self.sqlite3_vdbe_change_p4(addr, p4, p4type);
    addr
  }

  /// Change the value of the P4 operand for a specific instruction.
  pub fn sqlite3_vdbe_change_p4(&mut self, addr: i32, p4: String, n: P4Type) {
    let mut addr = addr;
    if addr < 0 {
      addr = (self.a_op.len() - 1) as i32;
    }
    let op = &self.a_op[addr as usize];
    if n.clone() as i32 >= 0 || op.p4type.is_some() {
      self.vdbe_change_p4_full(addr as usize, p4, n);
    }
    // TODO: Handle P4_INT32
  }

  pub fn add_op4_int(&mut self, op: Opcode, p1: i32, p2: i32, p3: i32, p4: i32) -> usize {
    let i = self.a_op.len();
    // TODO: if( p->nOpAlloc<=i ) ...
    self.a_op.push(VdbeOp {
      opcode: op,
      p1,
      p2,
      p3,
      p4type: Some(P4Type::Int32),
      p4: Some(P4Union::Int32(p4)),
      p5: None,
    });
    i
  }

  /// Change the value of the P4 operand for a specific instruction.
  pub fn vdbe_change_p4_full(&mut self, addr: usize, p4: String, n: P4Type) {
    if (n.clone() as i32) < 0 {
      self.sqlite3_vdbe_change_p4(addr as i32, p4, n);
    } else {
      // From the original source, if n is 0, the length of
      // p4 is calculated and n is updated, otherwise n is the
      // length of the string
      self.a_op[addr].p4 = Some(P4Union::String(p4));
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
    match &self.a_mem[self.result_row + i].value {
      MemValue::Undefined => String::from("undefined"),
      MemValue::Integer(x) => x.to_string(),
      MemValue::Real(x) => x.to_string(),
      MemValue::String(s) => s.to_string(),
    }
  }

  pub fn load_string(&mut self, dest: i32, strng: &str) -> i32 {
    self.sqlite3_add_op4(
      Opcode::String8,
      0,
      dest,
      0,
      strng.to_string(),
      P4Type::Transient,
    )
  }

  /// Execute as much of a VDBE program as we can.
  /// This is the core of sqlite3_step().
  fn sqlite3_vdbe_exec(&mut self) -> StepStatus {
    let n = self.a_op.len() as i32;
    let mut step_pc: usize = self.pc;

    #[allow(unused_assignments)]
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
        Opcode::String8 => {
          // TODO: handle encoding stuff
          // TODO: pOut = out2Prerelease(p, pOp);
          p_op.opcode = Opcode::String;
        }
        Opcode::String => {
          let out = &mut self.a_mem[p_op.p2 as usize];
          if let Some(P4Union::String(s)) = &p_op.p4 {
            out.value = MemValue::String(s.to_string())
          }
          step_pc += 1;
        }
        Opcode::Eq => {
          let in1 = &self.a_mem[p_op.p1 as usize];
          let in3 = &self.a_mem[p_op.p3 as usize];

          // TODO: affinity comparison
          let res = match (&in1.value, &in3.value) {
            (MemValue::Integer(a), MemValue::Integer(b)) => a == b,
            (MemValue::Integer(a), MemValue::Real(b)) => *a as f64 == *b,
            (MemValue::Real(a), MemValue::Integer(b)) => *a == *b as f64,
            (MemValue::Real(a), MemValue::Real(b)) => a == b,
            (MemValue::String(a), MemValue::String(b)) => a == b,
            _ => {
              panic!(
                "comparison between {:?} and {:?} not implemented yet",
                &in1.value, &in3.value
              )
            }
          };

          if res {
            step_pc = p_op.p2 as usize;
          } else {
            step_pc += 1;
          }
        }
        Opcode::ZeroOrNull => {
          // TODO: Check and set null
          self.a_mem[p_op.p2 as usize].value = MemValue::Integer(0);
          step_pc += 1;
        }
        Opcode::Add | Opcode::Subtract | Opcode::Multiply | Opcode::Divide | Opcode::Remainder => {
          let a = &self.a_mem[p_op.p1 as usize].value;
          let b = &self.a_mem[p_op.p2 as usize].value;

          let res = match p_op.opcode {
            Opcode::Add => {
              let mut res = 0;
              if let (MemValue::Integer(a_int), MemValue::Integer(b_int)) = (a, b) {
                res = *b_int + *a_int;
              }
              MemValue::Integer(res)
            }
            Opcode::Subtract => {
              let mut res = 0;
              if let (MemValue::Integer(a_int), MemValue::Integer(b_int)) = (a, b) {
                res = *b_int - *a_int;
              }
              MemValue::Integer(res)
            }
            Opcode::Multiply => {
              let mut res = 0;
              if let (MemValue::Integer(a_int), MemValue::Integer(b_int)) = (a, b) {
                res = *b_int * *a_int;
              }
              MemValue::Integer(res)
            }
            Opcode::Divide => {
              let mut res = 0;
              if let (MemValue::Integer(a_int), MemValue::Integer(b_int)) = (a, b) {
                res = *b_int / *a_int;
              }
              MemValue::Integer(res)
            }
            _ => {
              let mut res = 0;
              if let (MemValue::Integer(a_int), MemValue::Integer(b_int)) = (a, b) {
                res = *b_int % *a_int;
              }
              MemValue::Integer(res)
            }
          };
          let out = &mut self.a_mem[p_op.p3 as usize].value;
          *out = res;
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
        _ => {}
      }
    }

    // vdbe_return:
    status
  }

  /// Same as sqlite3Step
  /// Execute the statement pStmt, either until a row of data is ready, the
  /// statement is completely executed or an error occurs.
  pub fn sqlite3_step(&mut self) -> StepStatus {
    self.sqlite3_vdbe_exec()
  }
}
