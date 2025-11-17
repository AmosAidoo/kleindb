use crate::{
  compiler::{parser::expression::ExprList, tokenizer::token::Token},
  core::db::SQLite3,
  error::KleinDBError,
  vdbe::{
    mem::{MemValue, SQLite3Value},
    stmt::SQLite3Stmt,
  },
};

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
  pub db: &'a SQLite3,
  /// An engine for executing database bytecode
  pub vdbe: SQLite3Stmt,
  /// Number of memory cells used so far
  pub n_mem: usize,

  /// unqualified schema object name
  pub s_name_token: Option<Token<'a>>,

  /// OK to factor out constants
  pub ok_const_factor: bool,

  /// Constant expressions
  pub const_expr: Option<ExprList<'a>>,

  /// These fields available when isCreate is true
  pub cr: Cr,
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
  ) -> Result<(&'a Token<'a>, usize), KleinDBError> {
    let db = self.db;
    if !p_name2.text.is_empty() {
      // Same as token_type == Dummy
      if db.init.busy {
        // corrupt database
        return Err(KleinDBError::Corrupt);
      }

      let i_db = db.sqlite3_find_db(p_name1);

      if let Some(i) = i_db {
        Ok((p_name2, i))
      } else {
        // Come back to this later
        Err(KleinDBError::Error)
      }
    } else {
      // TODO: Look at this assert more closely
      //assert( db->init.iDb==0 || db->init.busy || IN_SPECIAL_PARSE
      //  || (db->mDbFlags & DBFLAG_Vacuum)!=0);
      Ok((p_name1, db.init.i_db.into()))
    }
  }

  /// Prepare a virtual machine for execution for the first time after
  /// creating the virtual machine. This involves things such
  /// as allocating registers and initializing the program counter.
  pub fn make_vdbe_ready(&mut self) {
    self.vdbe.a_mem.append(&mut vec![
      SQLite3Value {
        value: MemValue::Undefined
      };
      self.n_mem
    ]);
  }
}

#[derive(Debug, Default)]
pub struct Cr {
  pub addr_cr_tab: i32,
  pub reg_row_id: i32,
  pub reg_root: i32,
}
