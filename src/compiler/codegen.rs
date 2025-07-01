use crate::{SQLite3, VdbeOp, compiler::parser::SQLCmdList};

pub fn generate_bytecode(db: &SQLite3, ast: SQLCmdList) -> Vec<VdbeOp> {
  vec![]
}
