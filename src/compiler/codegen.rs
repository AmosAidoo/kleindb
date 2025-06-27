use crate::{SQLite3, VdbeOp, compiler::parser::SQLStmtList};

pub fn generate_bytecode(db: &SQLite3, ast: SQLStmtList) -> Vec<VdbeOp> {
  vec![]
}
