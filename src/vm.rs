use crate::{KleinDBContext, MemValue, Opcode, SQLite3Stmt};

pub enum StepStatus {
  Ok,
  Row,
  Done,
}

/// Execute as much of a VDBE program as we can.
/// This is the core of sqlite3_step().
fn sqlite3_vdbe_exec(vdbe: &mut SQLite3Stmt) -> StepStatus {
  let n = vdbe.a_op.len() as i32;
  let mut step_pc: usize = vdbe.pc;

  let mut status = StepStatus::Ok;
  loop {
    let p_op = &mut vdbe.a_op[step_pc];
    match p_op.opcode {
      Opcode::Init => {
        p_op.p1 += 1;
        // Jump to p2
        assert!(p_op.p2 > 0);
        assert!(p_op.p2 < n);
        step_pc = p_op.p2 as usize;
      }
      Opcode::Integer => {
        vdbe.a_mem[p_op.p2 as usize].value = MemValue::Integer(p_op.p1);
        step_pc += 1;
      }
      Opcode::ResultRow => {
        // TODO: bound checks

        vdbe.result_row = p_op.p1 as usize;
        vdbe.pc = step_pc + 1;
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
fn step(vdbe: &mut SQLite3Stmt) -> StepStatus {
  sqlite3_vdbe_exec(vdbe)
}

pub fn sqlite3_step(ctx: &mut KleinDBContext) -> Result<StepStatus, ()> {
  // TODO: Enter db mutex
  // TODO: Retry if schema changes
  if let Some(vdbe) = &mut ctx.vdbe {
    Ok(step(vdbe))
  } else {
    Err(())
  }
}
