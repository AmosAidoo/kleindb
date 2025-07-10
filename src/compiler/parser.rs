//! Trying to port grammar from parse.y
use chumsky::prelude::*;

use crate::{Opcode, Parse, Token, TokenType};

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
}

fn parse_expr<'a>()
-> impl Parser<'a, &'a [Token<'a>], Expr<'a>, extra::Err<Rich<'a, Token<'a>>>> + Clone {
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
-> impl Parser<'a, &'a [Token<'a>], ExprList<'a>, extra::Err<Rich<'a, Token<'a>>>> + Clone {
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

fn whitespace<'a>() -> impl Parser<'a, &'a [Token<'a>], (), extra::Err<Rich<'a, Token<'a>>>> + Clone
{
  any()
    .filter(|token: &Token| token.token_type == TokenType::Space)
    .ignored()
}

fn parse_oneselect<'a>()
-> impl Parser<'a, &'a [Token<'a>], Select<'a>, extra::Err<Rich<'a, Token<'a>>>> + Clone {
  let selcollist = parse_selcollist();

  any()
    .filter(|token: &Token| token.token_type == TokenType::SELECT)
    .ignored()
    .padded_by(whitespace().repeated())
    .then(selcollist)
    .map(|(_, expr_list)| Select { expr_list })
}

fn parse_select<'a>()
-> impl Parser<'a, &'a [Token<'a>], Select<'a>, extra::Err<Rich<'a, Token<'a>>>> + Clone {
  parse_oneselect()
}

pub fn parser<'a>()
-> impl Parser<'a, &'a [Token<'a>], SQLCmdList<'a>, extra::Err<Rich<'a, Token<'a>>>> {
  let semi = any().filter(|t: &Token| t.is_semi());

  let cmd = choice((
    semi.clone().map(|_| Cmd::Semi),
    parse_select()
      .then_ignore(semi.clone())
      .map(|node| Cmd::Select(node)),
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
  use crate::compiler::tokenizer;

  const SQL: &str = "SELECT 1, 2, 3;";

  #[test]
  fn test_parse_simple_select_into_ast() {
    let tokens = tokenizer::tokenize(SQL);
    let ast = parser().parse(&tokens).unwrap();
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
}
