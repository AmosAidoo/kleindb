use crate::{
  Parse, Token, TokenType,
  compiler::{
    codegen::sqlite3_expr_code_target,
    parser::{KleinDBParserError, match_token, whitespace},
  },
};
use chumsky::prelude::*;

#[derive(Debug, PartialEq, Clone)]
pub enum Expr<'a> {
  Null,
  Float(f64),
  Blob(&'a str),
  Integer(i32),
  String(&'a str),
  Variable,

  Add(Box<Expr<'a>>, Box<Expr<'a>>),
  Sub(Box<Expr<'a>>, Box<Expr<'a>>),
  Mul(Box<Expr<'a>>, Box<Expr<'a>>),
  Div(Box<Expr<'a>>, Box<Expr<'a>>),
  Mod(Box<Expr<'a>>, Box<Expr<'a>>),

  Equal(Box<Expr<'a>>, Box<Expr<'a>>),

  Concat(Box<Expr<'a>>, Box<Expr<'a>>),
  Collate,
}

impl<'a> Expr<'a> {
  pub fn code_temp(&self, p_parse: &mut Parse<'a>) -> i32 {
    // TODO: pExpr = sqlite3ExprSkipCollateAndLikely(pExpr)
    // TODO: if( ConstFactorOk(pParse)
    // && ALWAYS(pExpr!=0)
    // && pExpr->op!=TK_REGISTER
    // && sqlite3ExprIsConstantNotJoin(pParse, pExpr)
    // )
    if p_parse.ok_const_factor && self.is_constant_not_join() {
      self.run_just_once(p_parse, -1)
    } else {
      // For now, ignore the register reuse optimization
      // TODO: sqlite3GetTempReg(pParse)
      let r1 = p_parse.n_mem;
      p_parse.n_mem += 1;
      sqlite3_expr_code_target(p_parse, self, r1 as i32)
    }
  }

  // Generate code that will evaluate expression pExpr just one time
  // per prepared statement execution.
  pub fn run_just_once(&self, p_parse: &mut Parse<'a>, reg_dest: i32) -> i32 {
    // TODO: if( regDest<0 && p )
    // TODO: pExpr = sqlite3ExprDup(pParse->db, pExpr, 0);
    let mut reg_dest = reg_dest;

    // TODO: if( pExpr!=0 && ExprHasProperty(pExpr, EP_HasFunc) )
    if reg_dest < 0 {
      reg_dest = p_parse.n_mem as i32;
      p_parse.n_mem += 1;
    }
    let item = ExprListItem {
      p_expr: self.clone(),
      const_expr_reg: Some(reg_dest),
    };
    if p_parse.const_expr.is_none() {
      let mut res = ExprList { items: vec![] };
      res.items.push(item);
      p_parse.const_expr = Some(res);
    } else if let Some(ce) = p_parse.const_expr.as_mut() {
      ce.items.push(item)
    }
    reg_dest
  }

  // TODO
  pub fn is_constant_not_join(&self) -> bool {
    true
  }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExprListItem<'a> {
  /// parse tree for expression
  pub p_expr: Expr<'a>,

  pub const_expr_reg: Option<i32>,
}

/// A list of expressions.  Each expression may optionally have a
/// name. An expr/name combination can be used in several ways, such
/// as the list of "expr AS ID" fields following a "SELECT" or in the
/// list of "ID = expr" items in an UPDATE.
#[derive(Debug, PartialEq, Clone)]
pub struct ExprList<'a> {
  pub items: Vec<ExprListItem<'a>>,
}

pub fn parse_expr<'a>()
-> impl Parser<'a, &'a [Token<'a>], Expr<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  recursive(|expr| {
    let any_term = |tt: TokenType| any().filter(move |t: &Token| t.token_type == tt);

    let null = any_term(TokenType::NULL).map(|_| Expr::<'a>::Null);
    let float = any_term(TokenType::Float).map(|t| Expr::Float(t.text.parse().unwrap()));
    let blob = any_term(TokenType::Blob).map(|t| Expr::Blob(t.text));
    let integer = any_term(TokenType::Integer).map(|t| Expr::Integer(t.text.parse().unwrap()));
    let string = any_term(TokenType::String).map(|t| Expr::String(t.text));

    let term = choice((null, float, blob, string, integer));

    let atom = term
      .or(expr.delimited_by(
        match_token(TokenType::LeftParen),
        match_token(TokenType::RightParen),
      ))
      .padded_by(whitespace().repeated());

    let unary = atom;

    // * / %
    let product = unary.clone().foldl(
      match_token(TokenType::Star)
        .to(Expr::Mul as fn(_, _) -> _)
        .or(match_token(TokenType::Slash).to(Expr::Div as fn(_, _) -> _))
        .padded_by(whitespace().repeated())
        .then(unary)
        .repeated(),
      |lhs, (op, rhs): (fn(_, _) -> _, Expr)| op(Box::new(lhs), Box::new(rhs)),
    );

    // + -
    let sum = product.clone().foldl(
      match_token(TokenType::Plus)
        .to(Expr::Add as fn(_, _) -> _)
        .or(match_token(TokenType::Minus).to(Expr::Sub as fn(_, _) -> _))
        .padded_by(whitespace().repeated())
        .then(product)
        .repeated(),
      |lhs, (op, rhs): (fn(_, _) -> _, Expr)| op(Box::new(lhs), Box::new(rhs)),
    );

    // equality
    sum.clone().foldl(
      match_token(TokenType::Eq)
        .to(Expr::Equal as fn(_, _) -> _)
        .padded_by(whitespace().repeated())
        .then(sum)
        .repeated(),
      |lhs, (op, rhs): (fn(_, _) -> _, Expr)| op(Box::new(lhs), Box::new(rhs)),
    )
  })
}
