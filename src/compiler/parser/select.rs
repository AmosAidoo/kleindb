use crate::{
  Token, TokenType,
  compiler::parser::{
    KleinDBParserError,
    expression::{ExprList, ExprListItem, parse_expr},
    whitespace,
  },
};
use chumsky::prelude::*;

#[derive(Debug, PartialEq)]
pub struct Select<'a> {
  pub expr_list: ExprList<'a>,
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
          acc.items.push(ExprListItem {
            p_expr: curr,
            const_expr_reg: None,
            name: None,
          });
          acc
        })
    })
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

pub fn parse_select<'a>()
-> impl Parser<'a, &'a [Token<'a>], Select<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  parse_oneselect()
}
