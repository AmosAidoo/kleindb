//! Trying to port grammar from parse.y
pub mod create_table;
pub mod expression;
pub mod select;
pub mod update;

use crate::{Opcode, Parse, Token, TokenType, compiler::codegen::sqlite3_expr_code};
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
pub struct SQLCmdList<'a> {
  pub list: Vec<Cmd<'a>>,
}

/// A single SQL command
#[derive(Debug, PartialEq)]
pub enum Cmd<'a> {
  Semi,
  Select(select::Select<'a>),
  CreateTable(create_table::Table),
  Update(update::Update<'a>),
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

#[derive(Debug, PartialEq)]
struct ColumnDataType {
  dtype: Vec<String>,
  params: Vec<i32>,
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

/// The SrcItem object represents a single term in the FROM clause of a query.
/// The SrcList object is mostly an array of SrcItems.
#[derive(Debug, PartialEq)]
pub struct SrcItem {
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

#[derive(Debug, PartialEq)]
pub enum WhereOptRet<'a> {
  Empty,
  WhereExpr(expression::Expr<'a>),
  Returning,
}

fn where_opt_ret<'a>()
-> impl Parser<'a, &'a [Token<'a>], WhereOptRet<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let expr = expression::parse_expr().padded_by(whitespace().repeated());
  choice((
    match_token(TokenType::WHERE)
      .padded_by(whitespace().repeated())
      .ignore_then(expr)
      .map(|exp| WhereOptRet::WhereExpr(exp)),
    parse_empty().map(|t| WhereOptRet::Empty),
  ))
}

pub fn parser<'a>(
  p_parse: Arc<Mutex<Parse<'a>>>,
) -> impl Parser<'a, &'a [Token<'a>], SQLCmdList<'a>, extra::Err<KleinDBParserError<'a>>> {
  let semi = any().filter(|t: &Token| t.is_semi());

  let cmd = choice((
    semi.clone().map(|_| Cmd::Semi),
    select::parse_select()
      .then_ignore(semi.clone())
      .map(|node| Cmd::Select(node)),
    create_table::parse_create_table(Arc::clone(&p_parse))
      .then_ignore(semi.clone())
      .map(|node| Cmd::CreateTable(node)),
    update::parse_update(Arc::clone(&p_parse))
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
  {
    let vdbe = &mut p_parse.vdbe;

    vdbe.sqlite3_add_op0(Opcode::Halt);
    vdbe.sqlite3_vdbe_jump_here(0);
  }

  // Code constant expressions that were factored out of inner loops
  let const_expr = p_parse.const_expr.clone().unwrap();
  println!("const_expr: {:?}", const_expr);
  p_parse.ok_const_factor = false;
  for expr_item in const_expr.items.iter() {
    sqlite3_expr_code(
      p_parse,
      &expr_item.p_expr,
      expr_item.const_expr_reg.unwrap(),
    );
  }

  // Finally, jump back to the beginning of the executable code
  {
    let vdbe = &mut p_parse.vdbe;
    vdbe.sqlite3_vdbe_goto(1);
  }

  // Get the VDBE program ready for execution
  // TODO: if( pParse->nErr==0 )
  p_parse.make_vdbe_ready();
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    Cr, SQLite3, SQLite3Stmt,
    compiler::{
      parser::{
        expression::{Expr, ExprList, ExprListItem},
        select::Select,
      },
      tokenizer,
    },
  };

  fn create_ctx<'a>(db: &'a SQLite3) -> Arc<Mutex<Parse<'a>>> {
    // let db = SQLite3::new();
    Arc::new(Mutex::new(Parse {
      db: &db,
      vdbe: SQLite3Stmt::new(),
      n_mem: 0,
      s_name_token: None,
      const_expr: None,
      ok_const_factor: true,
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
            p_expr: Expr::Integer(1),
            const_expr_reg: None,
          },
          ExprListItem {
            p_expr: Expr::Integer(2),
            const_expr_reg: None,
          },
          ExprListItem {
            p_expr: Expr::Integer(3),
            const_expr_reg: None,
          },
        ],
      },
    })];
    assert_eq!(ast, SQLCmdList { list });
  }

  //   #[test]
  //   fn test_parse_create_table_start() {
  //     const SQL: &str = "CREATE TABLE t1";
  //     let db = SQLite3::new();
  //     let parse_ctx = create_ctx(&db);
  //     let tokens = tokenizer::tokenize(SQL);
  //     let ast = parse_create_table_start(parse_ctx).parse(&tokens).unwrap();

  //     assert_eq!(
  //       ast,
  //       Table {
  //         name: "t1".to_string(),
  //         p_key: None,
  //         schema: None,
  //         n_tab_ref: 1,
  //         a_col: vec![],
  //         i_db: 0
  //       }
  //     );
  //   }

  //   #[test]
  //   fn test_parse_create_table_end() {
  //     const SQL: &str = "(a)";
  //     let db = SQLite3::new();
  //     let parse_ctx = create_ctx(&db);
  //     let tokens = tokenizer::tokenize(SQL);
  //     let ast = parse_create_table_end().parse(&tokens).unwrap();

  //     assert_eq!(
  //       ast,
  //       ColumnList::Single(ColumnName {
  //         name: Token {
  //           text: "a",
  //           token_type: TokenType::Id
  //         },
  //         type_token: TypeToken::Empty
  //       })
  //     );
  //   }

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
