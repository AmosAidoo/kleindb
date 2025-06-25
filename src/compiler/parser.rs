//! Reading https://www.sqlite.org/lang.html
//! SQL As Understood By SQLite
use chumsky::prelude::*;

#[derive(Debug, PartialEq)]
pub enum SQLStmtList {
    SemiColon(Option<Box<SQLStmtList>>),
    SQLStmt(SQLStmt, Box<SQLStmtList>),
}

#[derive(Debug, PartialEq)]
pub enum SQLStmt {
    Select(SelectStmt),
}

#[derive(Debug, PartialEq)]
pub enum ResultColumn {
    Expr(Expr),
    Star,
}

#[derive(Debug, PartialEq)]
pub struct SelectStmt {
    result_column: ResultColumn,
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    LiteralValue(LiteralValue),
    Binary(Box<Expr>, BinaryOperator, Box<Expr>),
}

#[derive(Debug, PartialEq)]
pub enum LiteralValue {
    Numeric(f64),
    String,
    Blob,
    Null,
    True,
    False,
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

#[derive(Debug, PartialEq)]
pub enum BinaryOperator {
    Addition,
    Subtraction,
    Multiplication,
    Division,
}

#[derive(Debug)]
pub enum Keywords {
    Select,
}

fn digit_parser<'a>() -> impl Parser<'a, &'a str, char> + Clone {
    any().filter(|c: &char| c.is_ascii_digit())
}

fn numeric_literal_parser<'a>() -> impl Parser<'a, &'a str, String> + Clone {
    digit_parser().repeated().at_least(1).collect::<String>()
}

fn literal_value_parser<'a>() -> impl Parser<'a, &'a str, LiteralValue> + Clone {
    let numeric_literal =
        numeric_literal_parser().map(|num| LiteralValue::Numeric(num.parse().unwrap()));
    numeric_literal
}

fn expr_parser<'a>() -> impl Parser<'a, &'a str, Expr> + Clone {
    let literal_value = literal_value_parser().map(|lv| Expr::LiteralValue(lv));
    literal_value
}

fn result_column_parser<'a>() -> impl Parser<'a, &'a str, ResultColumn> + Clone {
    expr_parser().map(|expr| ResultColumn::Expr(expr))
}

fn select_stmt_parser<'a>() -> impl Parser<'a, &'a str, SelectStmt> + Clone {
    let result_column = result_column_parser();

    let select_stmt = text::ascii::keyword("SELECT")
        .padded()
        .ignore_then(result_column)
        .map(|result_column| SelectStmt { result_column });
    select_stmt
}

pub fn parser<'a>() -> impl Parser<'a, &'a str, SQLStmtList> {
    let select_stmt = select_stmt_parser();

    let sql_stmt = select_stmt.map(|select| SQLStmt::Select(select));

    let sql_stmt_list = recursive(|sql_stmt_list| {
        let stmt_with_semi_colon = sql_stmt
            .then_ignore(just(";"))
            .padded()
            .then(sql_stmt_list.clone().repeated().collect::<Vec<_>>())
            .map(|(sql_stmt, stmt_list)| {
                SQLStmtList::SQLStmt(
                    sql_stmt,
                    stmt_list.into_iter().rev().fold(
                        Box::new(SQLStmtList::SemiColon(None)),
                        |acc, curr| match curr {
                            SQLStmtList::SQLStmt(stmt, _) => {
                                Box::new(SQLStmtList::SQLStmt(stmt, acc))
                            }
                            SQLStmtList::SemiColon(list) => {
                                if let Some(lst) = list {
                                    Box::new(SQLStmtList::SemiColon(Some(lst)))
                                } else {
                                    Box::new(SQLStmtList::SemiColon(None))
                                }
                            }
                        },
                    ),
                )
            });
        
        let semi_colon_with_stmt = just(";")
            .padded()
            .then(sql_stmt_list.clone().repeated().collect::<Vec<_>>())
            .map(|(_semi, stmt_list)| {
                stmt_list
                    .into_iter()
                    .rev()
                    .fold(SQLStmtList::SemiColon(None), |acc, curr| match curr {
                        SQLStmtList::SQLStmt(stmt, _) => SQLStmtList::SQLStmt(stmt, Box::new(acc)),
                        SQLStmtList::SemiColon(list) => {
                            if let Some(lst) = list {
                                SQLStmtList::SemiColon(Some(lst))
                            } else {
                                SQLStmtList::SemiColon(None)
                            }
                        }
                    })
            });
        stmt_with_semi_colon.or(semi_colon_with_stmt)
    });

    sql_stmt_list
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digit() {
        let res = digit_parser().parse("1");
        assert_eq!(res.output(), Some(&'1'))
    }

    #[test]
    fn test_numeric_literal() {
        let res = numeric_literal_parser().parse("123");
        assert_eq!(res.output(), Some(&"123".to_string()))
    }

    fn create_test_result_column() -> ResultColumn {
        ResultColumn::Expr(Expr::LiteralValue(LiteralValue::Numeric(123.0)))
    }

    #[test]
    fn test_result_column() {
        let res = result_column_parser().parse("123");
        assert_eq!(res.output(), Some(&create_test_result_column()));
    }

    #[test]
    fn test_select_stmt() {
        let res = select_stmt_parser().parse("SELECT 123");
        let select_stmt = SelectStmt {
            result_column: create_test_result_column(),
        };
        assert_eq!(res.output(), Some(&select_stmt));
    }

    #[test]
    fn test_parser() {
        let res = parser().parse("SELECT 123;\n");
        let select_stmt = SelectStmt {
            result_column: create_test_result_column(),
        };
        let sql_stmt_list = SQLStmtList::SQLStmt(
            SQLStmt::Select(select_stmt),
            Box::new(SQLStmtList::SemiColon(None)),
        );
        assert_eq!(res.output(), Some(&sql_stmt_list));
    }
}
