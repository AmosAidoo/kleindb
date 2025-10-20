use crate::{Token, TokenType, compiler::parser::KleinDBParserError};
use chumsky::prelude::*;

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

pub fn parse_expr<'a>()
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
