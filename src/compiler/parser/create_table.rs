use crate::Parse;
use crate::Schema;
use crate::Token;
use crate::TokenType;
use crate::compiler::parser::Affinity;
use crate::compiler::parser::Column;
use crate::compiler::parser::ColumnDataType;
use crate::compiler::parser::KleinDBParserError;
use crate::compiler::parser::parse_dbnm;
use crate::compiler::parser::parse_name;
use crate::compiler::parser::whitespace;
use crate::sqlite3_name_from_token;
use chumsky::Parser;
use chumsky::prelude::*;
use std::sync::{Arc, Mutex};

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

        // TODO: Test for namespace collisio

        // TODO: Begin generating the code that will insert the table record into
        // the schema table.
        // Not sure if this should be moved into the codegen module for now
        Table {
          name: z_name.to_string(),
          p_key: None,
          // schema: Arc::clone(&db.a_db[i_db].schema.as_ref().unwrap()),
          schema: None,
          n_tab_ref: 1,
          a_col: vec![],
          i_db,
        }
      },
    )
}

fn parse_create_table_end<'a>()
-> impl Parser<'a, &'a [Token<'a>], ColumnList<'a>, extra::Err<KleinDBParserError<'a>>> + Clone {
  let lp = any().filter(|t: &Token| t.token_type == TokenType::LeftParen);
  let rp = any().filter(|t: &Token| t.token_type == TokenType::RightParen);

  let nm = parse_name();

  // ID or String
  let ids = any().filter(|t: &Token| matches!(t.token_type, TokenType::Id | TokenType::String));

  let typename_tail = ids
    .padded_by(whitespace().repeated())
    .map(|t: Token| TypeName::Multiple(Box::new(TypeName::Single(t)), None));

  // Returns a list of tokens representing the type.
  // For example, a type can be UNSIGNED BIG INT
  let typename = ids
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
      .ignore_then(number),
    number,
  ));
  let minus_num = any()
    .filter(|t: &Token| t.token_type == TokenType::Minus)
    .ignore_then(number);

  let signed = choice((plus_num, minus_num));

  let typetoken = choice((
    typename
      .clone()
      .then_ignore(lp.padded_by(whitespace().repeated()))
      .then(signed)
      .then_ignore(rp.padded_by(whitespace().repeated()))
      .map(|(tn, signed)| TypeToken::TypeNameWithSigned(tn, signed)),
    typename.clone().map(TypeToken::TypeName),
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
  let columnlist_tail = comma.ignore_then(
    columnname
      .clone()
      .map(|t: ColumnName| ColumnList::Multiple(Box::new(ColumnList::Single(t)), None)),
  );

  let columnlist = columnname
    .clone()
    .map(ColumnList::Single)
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

      let col = Column {
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

pub fn parse_create_table<'a>(
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
