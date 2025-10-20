//! Trying to port grammar from parse.y
use crate::{Opcode, Parse, Schema, Token, TokenType, sqlite3_name_from_token};
use bitflags::bitflags;
use chumsky::{
  DefaultExpected,
  error::Error,
  label::LabelError,
  prelude::*,
  util::{Maybe, MaybeRef},
};
use std::sync::{Arc, Mutex};

type Span = SimpleSpan<usize>;
#[derive(Debug, PartialEq)]
pub enum KleinDBParserError<'a> {
  ExpectedFound {
    span: Span,
    expected: Vec<DefaultExpected<'a, TokenType>>,
    found: Option<TokenType>,
  },
  InvalidFullDatabaseName(Span),
  InvalidWhitespace(Span),
  InvalidToken {
    span: Span,
    expected: TokenType,
    found: Option<TokenType>,
  },
}

impl<'a> Error<'a, &'a [Token<'a>]> for KleinDBParserError<'a> {
  fn merge(mut self, mut other: Self) -> Self {
    if let (
      Self::ExpectedFound { expected, .. },
      Self::ExpectedFound {
        expected: expected_other,
        ..
      },
    ) = (&mut self, &mut other)
    {
      expected.append(expected_other);
    }
    self
  }
}

impl<'a> LabelError<'a, &'a [Token<'a>], DefaultExpected<'a, Token<'a>>>
  for KleinDBParserError<'a>
{
  fn expected_found<E: IntoIterator<Item = DefaultExpected<'a, Token<'a>>>>(
    expected: E,
    found: Option<MaybeRef<'a, Token<'a>>>,
    span: Span,
  ) -> Self {
    Self::ExpectedFound {
      span,
      expected: expected
        .into_iter()
        .map(|exp| match exp {
          DefaultExpected::Token(maybe) => {
            DefaultExpected::<'a, TokenType>::Token(Maybe::Val(maybe.token_type.clone()))
          }
          DefaultExpected::Any => DefaultExpected::<'a, TokenType>::Any,
          DefaultExpected::SomethingElse => DefaultExpected::<'a, TokenType>::SomethingElse,
          _ => DefaultExpected::<'a, TokenType>::EndOfInput,
        })
        .collect(),
      found: found.as_deref().map(|f| f.token_type.clone()),
    }
  }
}

#[derive(Debug, PartialEq)]
pub struct Select<'a> {
  pub expr_list: ExprList<'a>,
}

/// Each node of an expression in the parse tree is an instance
/// of this structure.
#[derive(Debug, PartialEq)]
pub struct Expr<'a> {
  // combination of Expr.op and Expr.u.zToken
  pub token: Token<'a>,

  /// Left subnode
  pub p_left: Option<Box<Expr<'a>>>,

  /// Right subnode
  pub p_right: Option<Box<Expr<'a>>>,
}

#[derive(Debug, PartialEq)]
pub struct ExprListItem<'a> {
  /// parse tree for expression
  pub p_expr: Expr<'a>,
}

/// A list of expressions.  Each expression may optionally have a
/// name. An expr/name combination can be used in several ways, such
/// as the list of "expr AS ID" fields following a "SELECT" or in the
/// list of "ID = expr" items in an UPDATE.
#[derive(Debug, PartialEq)]
pub struct ExprList<'a> {
  pub items: Vec<ExprListItem<'a>>,
}

#[derive(Debug, PartialEq)]
pub struct SQLCmdList<'a> {
  pub list: Vec<Cmd<'a>>,
}

/// A single SQL command
#[derive(Debug, PartialEq)]
pub enum Cmd<'a> {
  Semi,
  Select(Select<'a>),
  CreateTable(Table),
  Update(Update<'a>),
}

bitflags! {
  #[derive(Debug, PartialEq)]
  pub struct ColFlags: u16 {
    const PRIMKEY   = 0x0001;   /* Column is part of the primary key */
    const HIDDEN    = 0x0002;   /* A hidden column in a virtual table */
    // Not used
    const HASTYPE   = 0x0004;   /* Type name follows column name */
    const UNIQUE    = 0x0008;   /* Column def contains "UNIQUE" or "PK" */
    const SORTERREF = 0x0010;   /* Use sorter-refs with this column */
    const VIRTUAL   = 0x0020;   /* GENERATED ALWAYS AS ... VIRTUAL */
    const STORED    = 0x0040;   /* GENERATED ALWAYS AS ... STORED */
    const NOTAVAIL  = 0x0080;   /* STORED column not yet calculated */
    const BUSY      = 0x0100;   /* Blocks recursion on GENERATED columns */
    // Not used
    const HASCOLL   = 0x0200;   /* Has collating sequence name in zCnName */
    const NOEXPAND  = 0x0400;   /* Omit this column when expanding "*" */
    const GENERATED = 0x0060;   /* Combo: _STORED, _VIRTUAL */
    const NOINSERT  = 0x0062;   /* Combo: _HIDDEN, _STORED, _VIRTUAL */
  }
}

#[derive(Debug, PartialEq)]
pub enum Affinity {
  None = 0x40,
  Blob = 0x41,
  Text = 0x42,
  Numeric = 0x43,
  Integer = 0x44,
  Real = 0x45,
  FlexNum = 0x46,
  Defer = 0x58,
}
/// Information about each column of an SQL table is held in an instance
/// of the Column structure, in the Table.a_col[] array.
#[derive(Debug, PartialEq)]
pub struct Column {
  ///  Name of this column
  name: String,
  ///  Datatype of this column
  datatype: Option<ColumnDataType>,
  ///  Collating sequence of this column
  collating_sequence: Option<String>,
  affinity: Affinity,
  /// Est size of value in this column. sizeof(INT)==1
  sz_est: u8,
  /// Column name hash for faster lookup
  h_name: u8,
  col_flags: Option<ColFlags>,
}

#[derive(Debug, PartialEq)]
pub struct Table {
  pub name: String,
  pub p_key: Option<i16>,
  pub schema: Option<Arc<Schema>>,
  pub n_tab_ref: usize,
  pub a_col: Vec<Column>,
  /// Database number to create the table in
  pub i_db: usize,
}

fn parse_expr<'a>()
-> impl Parser<'a, &'a [Token<'a>], Expr<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let any_term = |tt: TokenType| {
    any()
      .filter(move |t: &Token| t.token_type == tt)
      .map(|t| Expr {
        token: t,
        p_left: None,
        p_right: None,
      })
  };

  let null = any_term(TokenType::NULL);
  let float = any_term(TokenType::Float);
  let blob = any_term(TokenType::Blob);
  let integer = any_term(TokenType::Integer);
  let string = any_term(TokenType::String);

  let expr = recursive(|expr| choice((null, float, blob, string, integer)));

  expr
}

fn parse_selcollist<'a>()
-> impl Parser<'a, &'a [Token<'a>], ExprList<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let expr = parse_expr().padded_by(whitespace().repeated());
  let comma = any()
    .filter(|t: &Token<'a>| t.token_type == TokenType::Comma)
    .padded_by(whitespace().repeated());

  choice((expr.clone(), comma.ignore_then(expr)))
    .repeated()
    .at_least(1)
    .collect::<Vec<_>>()
    .map(|exprs| {
      exprs
        .into_iter()
        .fold(ExprList { items: vec![] }, |mut acc, curr| {
          acc.items.push(ExprListItem { p_expr: curr });
          acc
        })
    })
}

fn whitespace<'a>()
-> impl Parser<'a, &'a [Token<'a>], (), extra::Err<KleinDBParserError<'a>>> + Clone {
  any()
    .filter(|token: &Token| token.token_type == TokenType::Space)
    .ignored()
    .map_err(|e| {
      if let KleinDBParserError::ExpectedFound { span, .. } = e {
        KleinDBParserError::InvalidWhitespace(span)
      } else {
        e
      }
    })
}

fn parse_name<'a>()
-> impl Parser<'a, &'a [Token<'a>], Token<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  any()
    .filter(|t: &Token| {
      t.token_type == TokenType::String
        || t.token_type == TokenType::INDEXED
        || t.token_type == TokenType::JOIN
        || t.token_type == TokenType::Id
    })
    .map(|t| t)
}

fn parse_dbnm<'a>()
-> impl Parser<'a, &'a [Token<'a>], Token<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let nm = parse_name();
  choice((
    any()
      .filter(|t: &Token| t.token_type == TokenType::Dot)
      .ignore_then(nm.clone())
      .map(|nm_tok| nm_tok),
    empty::<&'a [Token<'a>], extra::Err<KleinDBParserError<'a>>>().map(|_| Token {
      text: "",
      token_type: TokenType::Dummy,
    }),
  ))
}

fn parse_oneselect<'a>()
-> impl Parser<'a, &'a [Token<'a>], Select<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let selcollist = parse_selcollist();

  any()
    .filter(|token: &Token| token.token_type == TokenType::SELECT)
    .ignored()
    .padded_by(whitespace().repeated())
    .then(selcollist)
    .map(|(_, expr_list)| Select { expr_list })
}

fn parse_select<'a>()
-> impl Parser<'a, &'a [Token<'a>], Select<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  parse_oneselect()
}

fn parse_create_table_start<'a>(
  p_parse: Arc<Mutex<Parse<'a>>>,
) -> impl Parser<'a, &'a [Token<'a>], Table, extra::Err<KleinDBParserError<'a>>> + Clone {
  let a_parse = Arc::clone(&p_parse);

  // TODO: add omit_tempdb feature flag
  let temp = choice((
    any::<&'a [Token<'a>], extra::Err<KleinDBParserError<'a>>>()
      .filter(|t: &Token| t.token_type == TokenType::TEMP)
      .padded_by(whitespace().repeated())
      .map(|_| true),
    empty::<&'a [Token<'a>], extra::Err<KleinDBParserError<'a>>>().map(|_| false),
  ));

  let ifnotexists = choice((
    any::<&'a [Token<'a>], extra::Err<KleinDBParserError<'a>>>()
      .filter(|t: &Token| t.token_type == TokenType::IF)
      .padded_by(whitespace().repeated())
      .then(
        any()
          .filter(|t: &Token| t.token_type == TokenType::NOT)
          .padded_by(whitespace().repeated()),
      )
      .then(
        any()
          .filter(|t: &Token| t.token_type == TokenType::EXISTS)
          .padded_by(whitespace().repeated()),
      )
      .map(|_| true),
    empty::<&'a [Token<'a>], extra::Err<KleinDBParserError<'a>>>().map(|_| false),
  ));

  let create_kw = any()
    .filter(|t: &Token| t.token_type == TokenType::CREATE)
    .padded_by(whitespace().repeated());

  let nm = parse_name();

  let dbnm = parse_dbnm();

  create_kw
    .ignore_then(temp)
    .then_ignore(
      any()
        .filter(|t: &Token| t.token_type == TokenType::TABLE)
        .padded_by(whitespace().repeated()),
    )
    .then(ifnotexists)
    .then(nm.clone().then(dbnm).padded_by(whitespace().repeated()))
    .map(
      // This closure attempts to implement sqlite3StartTable
      move |((_is_temp, _ifnotexists_is_set), (p_name1, p_name2)): (
        (bool, bool),
        (Token, Token),
      )| {
        let mut parse = a_parse.lock().unwrap();
        let db = parse.db;

        let (i_db, z_name, p_name) = if db.init.busy && db.init.new_t_num == 1 {
          // Special case:  Parsing the sqlite_schema or sqlite_temp_schema schema
          (
            db.init.i_db as usize,
            if db.init.i_db == 1 {
              "sqlite_temp_master"
            } else {
              "sqlite_master"
            },
            Some(p_name1),
          )
        } else {
          let tp_res = parse.sqlite3_two_part_name(&p_name1, &p_name2).unwrap();
          (tp_res.1, sqlite3_name_from_token(db, tp_res.0), None)
        };

        parse.s_name_token = p_name;

        // TODO: Test for namespace collision

        let table = Table {
          name: z_name.to_string(),
          p_key: None,
          // schema: Arc::clone(&db.a_db[i_db].schema.as_ref().unwrap()),
          schema: None,
          n_tab_ref: 1,
          a_col: vec![],
          i_db,
        };

        // TODO: Begin generating the code that will insert the table record into
        // the schema table.
        // Not sure if this should be moved into the codegen module for now
        table
      },
    )
}

#[derive(Debug, PartialEq)]
enum TypeName<'a> {
  Single(Token<'a>),
  Multiple(Box<TypeName<'a>>, Option<Box<TypeName<'a>>>),
}

#[derive(Debug, PartialEq)]
enum TypeToken<'a> {
  Empty,
  TypeName(TypeName<'a>),
  TypeNameWithSigned(TypeName<'a>, Token<'a>),
  // TypeNameWithTwoSigned(TypeName<'a>, Token<'a>, Token<'a>),
}

#[derive(Debug, PartialEq)]
struct ColumnName<'a> {
  name: Token<'a>,
  type_token: TypeToken<'a>,
}

#[derive(Debug, PartialEq)]
enum ColumnList<'a> {
  Single(ColumnName<'a>),
  Multiple(Box<ColumnList<'a>>, Option<Box<ColumnList<'a>>>),
}
fn parse_create_table_end<'a>()
-> impl Parser<'a, &'a [Token<'a>], ColumnList<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let lp = any().filter(|t: &Token| t.token_type == TokenType::LeftParen);
  let rp = any().filter(|t: &Token| t.token_type == TokenType::RightParen);

  let nm = parse_name();

  // ID or String
  let ids = any().filter(|t: &Token| matches!(t.token_type, TokenType::Id | TokenType::String));

  let typename_tail = ids
    .clone()
    .padded_by(whitespace().repeated())
    .map(|t: Token| TypeName::Multiple(Box::new(TypeName::Single(t)), None));

  // Returns a list of tokens representing the type.
  // For example, a type can be UNSIGNED BIG INT
  let typename = ids
    .clone()
    .padded_by(whitespace().repeated())
    .map(|t: Token| TypeName::Single(t))
    .foldl(typename_tail.repeated(), |tn, tail| {
      TypeName::Multiple(Box::new(tn), Some(Box::new(tail)))
    });

  let number =
    any().filter(|t: &Token| matches!(t.token_type, TokenType::Integer | TokenType::Float));

  let plus_num = choice((
    any()
      .filter(|t: &Token| t.token_type == TokenType::Plus)
      .ignore_then(number.clone()),
    number.clone(),
  ));
  let minus_num = any()
    .filter(|t: &Token| t.token_type == TokenType::Minus)
    .ignore_then(number);

  let signed = choice((plus_num, minus_num));

  let typetoken = choice((
    typename
      .clone()
      .then_ignore(lp.clone().padded_by(whitespace().repeated()))
      .then(signed)
      .then_ignore(rp.clone().padded_by(whitespace().repeated()))
      .map(|(tn, signed)| TypeToken::TypeNameWithSigned(tn, signed)),
    typename.clone().map(|t| TypeToken::TypeName(t)),
    empty().map(|_| TypeToken::Empty),
  ));

  let columnname = nm
    .padded_by(whitespace().repeated())
    .then(typetoken)
    .map(|(name, tt)| ColumnName {
      name,
      type_token: tt,
    });

  // TODO: carglist
  let comma = any().filter(|t: &Token| t.token_type == TokenType::Comma);
  let columnlist_tail = comma.clone().ignore_then(
    columnname
      .clone()
      .map(|t: ColumnName| ColumnList::Multiple(Box::new(ColumnList::Single(t)), None)),
  );

  let columnlist = columnname
    .clone()
    .map(|cn| ColumnList::Single(cn))
    .foldl(columnlist_tail.repeated(), |lhs, tail| {
      ColumnList::Multiple(Box::new(lhs), Some(Box::new(tail)))
    });

  choice((
    lp.padded_by(whitespace().repeated())
      .ignore_then(columnlist)
      // .then(conslist_opt)
      .then_ignore(rp.padded_by(whitespace().repeated()))
      .map(|cl: ColumnList| cl),
    // TODO: AS parse_select()
  ))
}

#[derive(Debug, PartialEq)]
struct ColumnDataType {
  dtype: Vec<String>,
  params: Vec<i32>,
}

// type ColumnDataType = Vec<String>;

fn unwrap_typename(tname: TypeName, res: &mut ColumnDataType) {
  match tname {
    TypeName::Single(single) => {
      res.dtype.push(single.text.to_string());
    }
    TypeName::Multiple(head, tail) => {
      unwrap_typename(*head, res);

      if let Some(tn) = tail {
        unwrap_typename(*tn, res);
      }
    }
  }
}

fn unwrap_columnlist(clist: ColumnList, res: &mut Vec<Column>) {
  match clist {
    ColumnList::Single(cn) => {
      let dtype: Option<ColumnDataType> = match cn.type_token {
        TypeToken::Empty => None,
        TypeToken::TypeName(tn) => {
          let mut col_dtype = ColumnDataType {
            dtype: vec![],
            params: vec![],
          };
          unwrap_typename(tn, &mut col_dtype);
          Some(col_dtype)
        }
        TypeToken::TypeNameWithSigned(tn, param) => {
          let mut col_dtype = ColumnDataType {
            dtype: vec![],
            params: vec![],
          };
          unwrap_typename(tn, &mut col_dtype);
          col_dtype.params.push(param.text.parse().unwrap());
          Some(col_dtype)
        }
      };

      let mut col = Column {
        name: cn.name.text.to_string(),
        datatype: dtype,
        collating_sequence: None,
        affinity: Affinity::Blob,
        sz_est: 1,
        h_name: 0,
        col_flags: None,
      };

      res.push(col);
    }
    ColumnList::Multiple(head, tail) => {
      // Expand left
      unwrap_columnlist(*head, res);

      // Expand right
      if let Some(t) = tail {
        unwrap_columnlist(*t, res);
      }
    }
  };
}

fn parse_create_table<'a>(
  p_parse: Arc<Mutex<Parse<'a>>>,
) -> impl Parser<'a, &'a [Token<'a>], Table, extra::Err<KleinDBParserError<'a>>> + Clone {
  let create_table = parse_create_table_start(Arc::clone(&p_parse));

  // Optional list of constraints after column definitions
  // let conslist_opt = empty();

  let create_table_args = parse_create_table_end();

  create_table.then(create_table_args).map(|(mut ct, cl)| {
    ct.a_col = vec![];

    // Add all the columns to a_col
    unwrap_columnlist(cl, &mut ct.a_col);

    ct
  })
}

/// The SrcItem object represents a single term in the FROM clause of a query.
/// The SrcList object is mostly an array of SrcItems.
#[derive(Debug, PartialEq)]
struct SrcItem {
  name: String,
  alias: Option<String>,
}

/// This object represents one or more tables that are the source of
/// content for an SQL statement.  For example, a single SrcList object
/// is used to hold the FROM clause of a SELECT statement.  SrcList also
/// represents the target tables for DELETE, INSERT, and UPDATE statements.
#[derive(Debug, PartialEq)]
pub struct SrcList {
  a: Vec<SrcItem>,
}

fn xfullname<'a>()
-> impl Parser<'a, &'a [Token<'a>], SrcList, extra::Err<KleinDBParserError<'a>>> + Clone {
  let nm = parse_name();
  nm.clone()
    .map(|t| SrcList {
      a: vec![SrcItem {
        alias: None,
        name: t.text.to_string(),
      }],
    })
    .map_err(|e| {
      if let KleinDBParserError::ExpectedFound { span, .. } = e {
        KleinDBParserError::InvalidFullDatabaseName(span)
      } else {
        e
      }
    })
}

fn match_token<'a>(
  tt: TokenType,
) -> impl Parser<'a, &'a [Token<'a>], Token<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let target = tt.clone();
  any()
    .filter(move |t: &Token| t.token_type == target)
    .map_err(move |e| {
      if let KleinDBParserError::ExpectedFound { span, found, .. } = e {
        KleinDBParserError::InvalidToken {
          span,
          expected: tt.clone(),
          found: found,
        }
      } else {
        e
      }
    })
}

#[derive(Debug, PartialEq)]
pub struct Update<'a> {
  pub table_name: SrcList,
  pub changes: ExprList<'a>,
  pub where_expr: Option<Expr<'a>>,
}

#[derive(Debug, PartialEq)]
pub enum SetList<'a> {
  // Base variants
  SingleAssignment { name: Token<'a>, expr: Expr<'a> },

  // Recursive variants
  Assignment(Box<SetList<'a>>, Option<Box<SetList<'a>>>),
}

fn parse_empty<'a>()
-> impl Parser<'a, &'a [Token<'a>], Token<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  empty::<&'a [Token<'a>], extra::Err<KleinDBParserError<'a>>>().map(|_| Token {
    text: "",
    token_type: TokenType::Dummy,
  })
}

fn parse_from<'a>()
-> impl Parser<'a, &'a [Token<'a>], Token<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  parse_empty()
}

pub enum WhereOptRet<'a> {
  Empty,
  WhereExpr(Expr<'a>),
  Returning,
}

fn where_opt_ret<'a>()
-> impl Parser<'a, &'a [Token<'a>], WhereOptRet<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let expr = parse_expr().padded_by(whitespace().repeated());
  choice((
    match_token(TokenType::WHERE)
      .padded_by(whitespace().repeated())
      .ignore_then(expr)
      .map(|exp| WhereOptRet::WhereExpr(exp)),
    parse_empty().map(|t| WhereOptRet::Empty),
  ))
}

fn parse_update<'a>(
  p_parse: Arc<Mutex<Parse<'a>>>,
) -> impl Parser<'a, &'a [Token<'a>], Update<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let expr = parse_expr().padded_by(whitespace().repeated());
  let nm = parse_name();

  let assignment = nm
    .padded_by(whitespace().repeated())
    .then_ignore(match_token(TokenType::Eq).padded_by(whitespace().repeated()))
    .then(expr)
    .map(|(lhs, rhs)| {
      SetList::SingleAssignment {
        name: lhs,
        expr: rhs,
      }
    });

  let assignment_tail = match_token(TokenType::Comma)
    .padded_by(whitespace().repeated())
    .ignore_then(assignment.clone())
    .map(|tail| SetList::Assignment(Box::new(tail), None));

  let setlist = assignment
    .clone()
    .foldl(assignment_tail.repeated(), |init, tail| {
      SetList::Assignment(Box::new(init), Some(Box::new(tail)))
    });

  match_token(TokenType::UPDATE)
    .padded_by(whitespace().repeated())
    .ignore_then(xfullname().padded_by(whitespace().repeated()))
    .then_ignore(match_token(TokenType::SET).padded_by(whitespace().repeated()))
    .then(setlist)
    .then(parse_from())
    .then(where_opt_ret().padded_by(whitespace().repeated()))
    .map(
      |(((name, setlst), _from), where_clause): (
        ((SrcList, SetList<'_>), Token<'_>),
        WhereOptRet<'_>,
      )| {
        Update {
          table_name: name,
          changes: ExprList { items: vec![] },
          where_expr: match where_clause {
            WhereOptRet::Empty => None,
            WhereOptRet::WhereExpr(expr) => Some(expr),
            WhereOptRet::Returning => None,
          },
        }
      },
    )
}

pub fn parser<'a>(
  p_parse: Arc<Mutex<Parse<'a>>>,
) -> impl Parser<'a, &'a [Token<'a>], SQLCmdList<'a>, extra::Err<KleinDBParserError<'a>>> {
  let semi = any().filter(|t: &Token| t.is_semi());

  let cmd = choice((
    semi.clone().map(|_| Cmd::Semi),
    parse_select()
      .then_ignore(semi.clone())
      .map(|node| Cmd::Select(node)),
    parse_create_table(Arc::clone(&p_parse))
      .then_ignore(semi.clone())
      .map(|node| Cmd::CreateTable(node)),
    parse_update(Arc::clone(&p_parse))
      .then_ignore(semi.clone())
      .map(|node| Cmd::Update(node)),
  ))
  .padded_by(whitespace().repeated())
  .repeated()
  .at_least(1)
  .collect::<Vec<_>>()
  .map(|cmds| SQLCmdList {
    list: cmds
      .into_iter()
      .filter(|cmd| !matches!(cmd, Cmd::Semi))
      .collect(),
  });

  cmd
}

/// This routine is called after a single SQL statement has been
/// parsed and a VDBE program to execute that statement has been
/// prepared.  This routine puts the finishing touches on the
/// VDBE program and resets the pParse structure for the next
/// parse.
pub fn sqlite3_finish_coding(p_parse: &mut Parse) {
  let vdbe = &mut p_parse.vdbe;

  vdbe.sqlite3_add_op0(Opcode::Halt);
  vdbe.sqlite3_vdbe_jump_here(0);

  // Finally, jump back to the beginning of the executable code
  vdbe.sqlite3_vdbe_goto(1);
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Cr, SQLite3, SQLite3Stmt, compiler::tokenizer};

  fn create_ctx<'a>(db: &'a SQLite3) -> Arc<Mutex<Parse<'a>>> {
    // let db = SQLite3::new();
    Arc::new(Mutex::new(Parse {
      db: &db,
      vdbe: SQLite3Stmt::new(),
      n_mem: 0,
      s_name_token: None,
      cr: Cr {
        addr_cr_tab: -1,
        reg_row_id: -1,
        reg_root: -1,
      },
    }))
  }

  #[test]
  fn test_parse_simple_select_into_ast() {
    const SQL: &str = "SELECT 1, 2, 3;";
    let db = SQLite3::new();
    let parse_ctx = create_ctx(&db);
    let tokens = tokenizer::tokenize(SQL);
    let ast = parser(parse_ctx).parse(&tokens).unwrap();
    let list = vec![Cmd::Select(Select {
      expr_list: ExprList {
        items: vec![
          ExprListItem {
            p_expr: Expr {
              token: Token {
                text: "1",
                token_type: TokenType::Integer,
              },
              p_left: None,
              p_right: None,
            },
          },
          ExprListItem {
            p_expr: Expr {
              token: Token {
                text: "2",
                token_type: TokenType::Integer,
              },
              p_left: None,
              p_right: None,
            },
          },
          ExprListItem {
            p_expr: Expr {
              token: Token {
                text: "3",
                token_type: TokenType::Integer,
              },
              p_left: None,
              p_right: None,
            },
          },
        ],
      },
    })];
    assert_eq!(ast, SQLCmdList { list });
  }

  #[test]
  fn test_parse_create_table_start() {
    const SQL: &str = "CREATE TABLE t1";
    let db = SQLite3::new();
    let parse_ctx = create_ctx(&db);
    let tokens = tokenizer::tokenize(SQL);
    let ast = parse_create_table_start(parse_ctx).parse(&tokens).unwrap();

    assert_eq!(
      ast,
      Table {
        name: "t1".to_string(),
        p_key: None,
        schema: None,
        n_tab_ref: 1,
        a_col: vec![],
        i_db: 0
      }
    );
  }

  #[test]
  fn test_parse_create_table_end() {
    const SQL: &str = "(a)";
    let db = SQLite3::new();
    let parse_ctx = create_ctx(&db);
    let tokens = tokenizer::tokenize(SQL);
    let ast = parse_create_table_end().parse(&tokens).unwrap();

    assert_eq!(
      ast,
      ColumnList::Single(ColumnName {
        name: Token {
          text: "a",
          token_type: TokenType::Id
        },
        type_token: TypeToken::Empty
      })
    );
  }

  // #[test]
  // fn test_parse_create_table_into_ast() {
  //   const SQL: &str = "CREATE TABLE t1(a);";
  //   let db = SQLite3::new();
  //   let parse_ctx = create_ctx(&db);
  //   let tokens = tokenizer::tokenize(SQL);
  //   let ast = parser(parse_ctx).parse(&tokens);
  //   println!("{ast:?}");
  //   // assert_eq!(ast, SQLCmdList { list });
  //   assert!(true);
  // }
}
