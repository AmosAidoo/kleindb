use crate::{
  Parse, Token, TokenType,
  compiler::parser::{
    KleinDBParserError, SrcList, WhereOptRet,
    expression::{Expr, ExprList, parse_expr},
    match_token, parse_from, parse_name, where_opt_ret, whitespace, xfullname,
  },
};
use chumsky::prelude::*;
use std::sync::{Arc, Mutex};

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

pub fn parse_update<'a>(
  _p_parse: Arc<Mutex<Parse<'a>>>,
) -> impl Parser<'a, &'a [Token<'a>], Update<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let expr = parse_expr().padded_by(whitespace().repeated());
  let nm = parse_name();

  let assignment = nm
    .padded_by(whitespace().repeated())
    .then_ignore(match_token(TokenType::Eq).padded_by(whitespace().repeated()))
    .then(expr)
    .map(|(lhs, rhs)| SetList::SingleAssignment {
      name: lhs,
      expr: rhs,
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
      |(((name, _setlst), _from), where_clause): (
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
