use crate::{KleinDBContext, Opcode, SQLite3Stmt};

/// Execute as much of a VDBE program as we can.
/// This is the core of sqlite3_step().
fn sqlite3_vdbe_exec(vdbe: &mut SQLite3Stmt) {}

/// Same as sqlite3Step
/// Execute the statement pStmt, either until a row of data is ready, the
/// statement is completely executed or an error occurs.
fn step(vdbe: &mut SQLite3Stmt) {
  loop {
    let p_op = &vdbe.a_op[vdbe.pc];

    match p_op.opcode {
      Opcode::Init => todo!(),
      Opcode::Integer => todo!(),
      Opcode::ResultRow => todo!(),
      Opcode::Halt => todo!(),
      Opcode::Goto => todo!(),
    }
  }
}

fn sqlite3_step(ctx: &mut KleinDBContext) {
  // TODO: Enter db mutex
  // TODO: Retry if schema changes
  if let Some(vdbe) = &mut ctx.vdbe {
    step(vdbe);
  }

  // step(p_stmt);
}
