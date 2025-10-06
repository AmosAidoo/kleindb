use crate::{
  OpFlags, Opcode, P4Type, Parse, SQLite3, SQLite3Stmt, SelectDest, SelectResultType,
  TextEncodings, TokenType, VdbeOp,
  compiler::parser::{Cmd, Expr, ExprList, SQLCmdList, Select, Table, sqlite3_finish_coding},
  storage::btree::{BtreeConstants, BtreeCreateTableFlags},
};

/// Generate an instruction that will put the integer describe by
/// text z[0..n-1] into register iMem.
fn code_integer(p_parse: &mut Parse, expr: &Expr, neg_flag: bool, i_mem: i32) {
  let vdbe = &mut p_parse.vdbe;
  let i: i32 = expr.token.text.parse().unwrap();
  let i = if neg_flag { -i } else { i };
  vdbe.sqlite3_add_op2(Opcode::Integer, i, i_mem);
}

/// Generate code into the current Vdbe to evaluate the given
/// expression.  Attempt to store the results in register "target".
/// Return the register where results are stored.
///
/// With this routine, there is no guarantee that results will
/// be stored in target.  The result might be stored in some other
/// register if it is convenient to do so.  The calling function
/// must check the return code and move the results to the desired
/// register.
fn sqlite3_expr_code_target(p_parse: &mut Parse, expr: &Expr, target: i32) -> i32 {
  match expr.token.token_type {
    TokenType::Integer => {
      code_integer(p_parse, expr, false, target);
      target
    }
    _ => -1,
  }
}

/// Generate code that pushes the value of every element of the given
/// expression list into a sequence of registers beginning at target.
fn sqlite3_expr_code_expr_list(p_parse: &mut Parse, p_list: &ExprList, target: i32) -> usize {
  let vdbe = &mut p_parse.vdbe;
  let mut n = p_list.items.len();
  for (i, p_item) in p_list.items.iter().enumerate() {
    let expr = &p_item.p_expr;

    let in_reg = sqlite3_expr_code_target(p_parse, expr, target + i as i32);
    if in_reg != target + i as i32 {
      // TODO
    }
  }

  n
}

struct RowLoadInfo {
  pub reg_result: usize,
  pub ecel_flags: u8,
}

/// This routine does the work of loading query data into an array of
/// registers so that it can be added to the sorter.
fn inner_loop_load_row(p_parse: &mut Parse, select: Select, p_info: &RowLoadInfo) {
  sqlite3_expr_code_expr_list(p_parse, &select.expr_list, p_info.reg_result as i32);
}

const SQLITE_ECEL_DUP: u8 = 0x01; /* Deep, not shallow copies */
const SQLITE_ECEL_FACTOR: u8 = 0x02; /* Factor out constant terms */
const SQLITE_ECEL_REF: u8 = 0x04; /* Use ExprList.u.x.iOrderByCol */
const SQLITE_ECEL_OMITREF: u8 = 0x08; /* Omit if ExprList.u.x.iOrderByCol */

/// This routine generates the code for the inside of the inner loop
/// of a SELECT.
///
/// If srcTab is negative, then the p->pEList expressions
/// are evaluated in order to get the data for this row.  If srcTab is
/// zero or more, then data is pulled from srcTab and p->pEList is used only
/// to get the number of columns and the collation sequence for each column.
fn select_inner_loop(p_parse: &mut Parse, select: Select, src_tab: i32, dest: &mut SelectDest) {
  let mut s_row_load_info = RowLoadInfo {
    reg_result: 0,
    ecel_flags: 0,
  };

  // Pull the requested columns
  let n_result_col = select.expr_list.items.len();
  let e_dest = &dest.e_dest;

  if dest.i_sdst == 0 {
    dest.i_sdst = p_parse.n_mem + 1;
    p_parse.n_mem += n_result_col;
  }

  dest.n_sdst = n_result_col;
  // Start of memory holding full result (or 0)
  let reg_origin = dest.i_sdst;
  // Start of memory holding current results
  let reg_result = dest.i_sdst;

  if src_tab >= 0 {
    //
  } else if !matches!(e_dest, SelectResultType::Exists) {
    // "ecel" is an abbreviation of "ExprCodeExprList"
    let mut ecel_flags: u8 = if matches!(
      e_dest,
      SelectResultType::Mem | SelectResultType::Output | SelectResultType::Coroutine
    ) {
      SQLITE_ECEL_DUP
    } else {
      0
    };

    s_row_load_info.reg_result = reg_result;
    s_row_load_info.ecel_flags = ecel_flags;
    // below is the else of this condition
    // if( p->iLimit
    //   && (ecelFlags & SQLITE_ECEL_OMITREF)!=0
    //   && nPrefixReg>0
    //  )
    inner_loop_load_row(p_parse, select, &s_row_load_info);

    let vdbe = &mut p_parse.vdbe;
    match e_dest {
      SelectResultType::Coroutine | SelectResultType::Output => {
        vdbe.sqlite3_add_op2(Opcode::ResultRow, reg_result as i32, n_result_col as i32);
      }
      _ => {}
    }
  }
}

/// Generate byte-code for the SELECT statement
fn sqlite3_select(p_parse: &mut Parse, select: Select, dest: &mut SelectDest) {
  // sqlite3GenerateColumnNames does this but not implemented yet
  p_parse.vdbe.n_res_column = select.expr_list.items.len();

  select_inner_loop(p_parse, select, -1, dest);
}

/// Generate VDBE code that prepares for doing an operation that
/// might change the database
fn begin_write_operation(p_parse: &mut Parse) {}

/// Generate byte-code for CREATE TABLE
fn generate_create_table(p_parse: &mut Parse, table: Table) {
  // Begin generating the code that will insert the table record into
  // the schema table.
  begin_write_operation(p_parse);

  let vdbe = &mut p_parse.vdbe;
  let null_row = [6, 0, 0, 0, 0, 0].map(|c| c as u8 as char).iter().collect();

  // TODO: check omit_virtualtable feature flag

  // If the file format and encoding in the database have not been set,
  // set them now.
  p_parse.n_mem += 1;
  let reg1 = p_parse.n_mem;
  p_parse.cr.reg_row_id = reg1 as i32;
  p_parse.n_mem += 1;
  let reg2 = p_parse.n_mem;
  p_parse.cr.reg_root = reg2 as i32;
  p_parse.n_mem += 1;
  let reg3 = p_parse.n_mem;
  vdbe.sqlite3_add_op3(
    Opcode::ReadCookie,
    table.i_db as i32,
    reg3 as i32,
    BtreeConstants::FileFormat as i32,
  );
  // TODO: sqlite3VdbeUsesBtree
  let addr1 = vdbe.sqlite3_add_op1(Opcode::If, reg3 as i32);
  // TODO: Undetstand the file format numbers
  let fileformat = 1;
  vdbe.sqlite3_add_op3(
    Opcode::SetCookie,
    table.i_db as i32,
    BtreeConstants::FileFormat as i32,
    fileformat,
  );
  vdbe.sqlite3_add_op3(
    Opcode::SetCookie,
    table.i_db as i32,
    BtreeConstants::TextEncoding as i32,
    TextEncodings::Utf8 as i32,
  );
  vdbe.sqlite3_vdbe_jump_here(addr1);

  // This just creates a place-holder record in the sqlite_schema table.
  // The record created does not contain anything yet.  It will be replaced
  // by the real entry in code generated at sqlite3EndTable().
  // TODO: if( isView || isVirtual )...
  p_parse.cr.addr_cr_tab = vdbe.sqlite3_add_op3(
    Opcode::CreateBtree,
    table.i_db as i32,
    reg2 as i32,
    BtreeCreateTableFlags::IntKey as i32,
  ) as i32;
  // TODO: sqlite3OpenSchemaTable()
  vdbe.sqlite3_add_op2(Opcode::NewRowid, 0, reg1 as i32);
  vdbe.sqlite3_add_op4(Opcode::Blob, 6, reg3 as i32, 0, null_row, P4Type::Static);
  vdbe.sqlite3_add_op3(Opcode::Insert, 0, reg3 as i32, reg1 as i32);
  vdbe.sqlite3_vdbe_change_p5(OpFlags::APPEND);
  vdbe.sqlite3_add_op0(Opcode::Close);
}

// Generates bytecode for every SQL statement in the parse tree
pub fn generate_bytecode<'a>(p_parse: &mut Parse, ast: SQLCmdList<'a>) {
  for cmd in ast.list {
    match cmd {
      Cmd::Select(select) => {
        let mut dest = SelectDest {
          e_dest: SelectResultType::Output,
          i_sdst: 0,
          n_sdst: 0,
        };
        sqlite3_select(p_parse, select, &mut dest);
      }
      Cmd::CreateTable(table) => {
        generate_create_table(p_parse, table);
      }
      // This case never happens as it is filtered out
      Cmd::Semi => {}
    }

    sqlite3_finish_coding(p_parse);
  }
}
