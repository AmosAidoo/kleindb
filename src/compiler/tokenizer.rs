use crate::{SQLITE_DIGIT_SEPARATOR, Token, TokenType, is_id_char};

// Character classes for tokenizing
enum CharacterClasses {
  /// The letter 'x', or start of BLOB literal
  X,
  /// First letter of a keyword
  Kywd0,
  /// Alphabetics or '_'.  Usable in a keyword
  Kywd,
  /// Digits
  Digit,
  /// '$'
  Dollar,
  /// '@', '#', ':'.  Alphabetic SQL variables
  VarAlpha,
  /// '?'.  Numeric SQL variables
  VarNum,
  Space,
  Quote,
  Quote2,
  Pipe,
  Minus,
  Lt,
  Gt,
  Eq,
  Bang,
  Slash,
  LeftParen,
  RightParen,
  Semi,
  Plus,
  Star,
  Percent,
  Comma,
  And,
  Tilda,

  Dot,
  /// unicode characters usable in IDs
  Id,
  /// Illegal character
  Illegal,
  // 0x00
  Nul,
  /// First byte of UTF8 BOM:  0xEF 0xBB 0xBF
  Bom,
}

impl From<u8> for CharacterClasses {
  fn from(value: u8) -> Self {
    match value {
      0 => Self::X,
      1 => Self::Kywd0,
      2 => Self::Kywd,
      3 => Self::Digit,
      4 => Self::Dollar,
      5 => Self::VarAlpha,
      6 => Self::VarNum,
      7 => Self::Space,
      8 => Self::Quote,
      9 => Self::Quote2,
      10 => Self::Pipe,
      11 => Self::Minus,
      12 => Self::Lt,
      13 => Self::Gt,
      14 => Self::Eq,
      15 => Self::Bang,
      16 => Self::Slash,
      17 => Self::LeftParen,
      18 => Self::RightParen,
      19 => Self::Semi,
      20 => Self::Plus,
      21 => Self::Star,
      22 => Self::Percent,
      23 => Self::Comma,
      24 => Self::And,
      25 => Self::Tilda,
      26 => Self::Dot,
      27 => Self::Id,
      28 => Self::Illegal,
      29 => Self::Nul,
      _ => Self::Bom,
    }
  }
}

/// To speed up lookup of character class given initial
/// character
const AI_CLASS: [u8; 256] = [
  29, 28, 28, 28, 28, 28, 28, 28, 28, 7, 7, 28, 7, 7, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28,
  28, 28, 28, 28, 28, 28, 28, 7, 15, 8, 5, 4, 22, 24, 8, 17, 18, 21, 20, 23, 11, 26, 16, 3, 3, 3,
  3, 3, 3, 3, 3, 3, 3, 5, 19, 12, 14, 13, 6, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 0, 2, 2, 9, 28, 28, 28, 2, 8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 0, 2, 2, 28, 10, 28, 25, 28, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
  27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
  27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
  27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
  27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
  27, 27, 27, 30, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
];

pub fn tokenize<'a>(sql: &'a str) -> Vec<Token<'a>> {
  let mut tokens: Vec<Token> = vec![];
  let mut i: usize = 0;
  let n = sql.len();
  let sql_bytes: Vec<u8> = sql.bytes().collect();
  while i < n {
    let cc: CharacterClasses = AI_CLASS[sql_bytes[i] as usize].into();

    match cc {
      CharacterClasses::Space => {
        // matches!(z, b' ' | b'\t' | b'\n' | 0x0c | b'\r');
        let prev = i;
        while i < n && sql_bytes[i].is_ascii_whitespace() {
          i += 1;
        }
        tokens.push(Token {
          text: &sql[prev..i],
          token_type: TokenType::Space,
        });
      }
      CharacterClasses::Minus => {
        if sql_bytes[i + 1] == b'-' {
          let prev = i;
          i += 2;
          while i < n && sql_bytes[i] != b'\n' {
            i += 1;
          }
          tokens.push(Token {
            text: &sql[prev..i],
            token_type: TokenType::Comment,
          });
        } else if sql_bytes[i + 1] == b'>' {
          let prev = i;
          i += 2 + if sql_bytes[i + 2] == b'>' { 1 } else { 0 };
          tokens.push(Token {
            text: &sql[prev..i],
            token_type: TokenType::Ptr,
          });
        } else {
          tokens.push(Token {
            text: &sql[i..i + 1],
            token_type: TokenType::Minus,
          });
          i += 1;
        }
      }
      CharacterClasses::LeftParen => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::LeftParen,
        });
        i += 1;
      }
      CharacterClasses::RightParen => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::RightParen,
        });
        i += 1;
      }
      CharacterClasses::Semi => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Semi,
        });
        i += 1;
      }
      CharacterClasses::Plus => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Plus,
        });
        i += 1;
      }
      CharacterClasses::Star => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Star,
        });
        i += 1;
      }
      CharacterClasses::Slash => {
        let prev = i;
        if sql_bytes[i + 1] != b'*' || i + 2 >= n {
          tokens.push(Token {
            text: &sql[i..i + 1],
            token_type: TokenType::Slash,
          });
          i += 1;
          continue;
        }
        i += 2;
        while i + 1 < n && (sql_bytes[i] != b'*' || sql_bytes[i + 1] != b'/') {
          i += 1;
        }

        if i + 1 < n {
          // We found end of comment
          i += 2;
        }

        tokens.push(Token {
          text: &sql[prev..i],
          token_type: TokenType::Comment,
        });
      }
      CharacterClasses::Percent => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Rem,
        });
      }
      CharacterClasses::Eq => {
        let prev = i;
        if sql_bytes[i + 1] == b'=' {
          i += 1;
        }
        tokens.push(Token {
          text: &sql[prev..i + 1],
          token_type: TokenType::Eq,
        });
        i += 1;
      }
      CharacterClasses::Lt => {
        let token_type = match sql_bytes[i + 1] {
          b'=' => TokenType::LE,
          b'>' => TokenType::NE,
          b'<' => TokenType::LShift,
          _ => TokenType::LT,
        };

        let token = match token_type {
          TokenType::LT => {
            let token = Token {
              text: &sql[i..i + 1],
              token_type: token_type,
            };
            i += 1;
            token
          }
          _ => {
            let token = Token {
              text: &sql[i..i + 2],
              token_type: token_type,
            };
            i += 2;
            token
          }
        };

        tokens.push(token);
      }
      CharacterClasses::Gt => {
        let token_type = match sql_bytes[i + 1] {
          b'=' => TokenType::GE,
          b'>' => TokenType::RShift,
          _ => TokenType::GT,
        };

        let token = match token_type {
          TokenType::GT => {
            let token = Token {
              text: &sql[i..i + 1],
              token_type: token_type,
            };
            i += 1;
            token
          }
          _ => {
            let token = Token {
              text: &sql[i..i + 2],
              token_type: token_type,
            };
            i += 2;
            token
          }
        };

        tokens.push(token);
      }
      CharacterClasses::Bang => {
        if sql_bytes[i + 1] != b'=' {
          tokens.push(Token {
            text: &sql[i..i + 1],
            token_type: TokenType::Illegal,
          });
          i += 1;
        } else {
          tokens.push(Token {
            text: &sql[i..i + 2],
            token_type: TokenType::NE,
          });
          i += 2;
        }
      }
      CharacterClasses::Pipe => {
        if sql_bytes[i + 1] != b'|' {
          tokens.push(Token {
            text: &sql[i..i + 1],
            token_type: TokenType::BitOr,
          });
          i += 1;
        } else {
          tokens.push(Token {
            text: &sql[i..i + 1],
            token_type: TokenType::Concat,
          });
          i += 2;
        }
      }
      CharacterClasses::Comma => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Comma,
        });
        i += 1;
      }
      CharacterClasses::And => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::BitAnd,
        });
        i += 1;
      }
      CharacterClasses::Tilda => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::BitNot,
        });
        i += 1;
      }
      CharacterClasses::Quote => {
        let prev = i;
        let delim = sql_bytes[i];
        // assert!(matches!(delim, b'`' | b'\'' | b'"'));
        i += 1;
        let mut c: u8 = 0;
        while i < n {
          c = sql_bytes[i];
          if c == delim {
            if i + 1 < n && sql_bytes[i + 1] == delim {
              i += 1;
            } else {
              break;
            }
          }
          i += 1;
        }
        if c == b'\'' {
          tokens.push(Token {
            text: &sql[prev..i + 1],
            token_type: TokenType::String,
          });
          i += 1;
        } else if i < n {
          tokens.push(Token {
            text: &sql[prev..i + 1],
            token_type: TokenType::Id,
          });
          i += 1;
        } else {
          tokens.push(Token {
            text: &sql[prev..i],
            token_type: TokenType::Illegal,
          });
          i += 1;
        }
      }
      CharacterClasses::Dot => {
        if !sql_bytes[i + 1].is_ascii_digit() {
          tokens.push(Token {
            text: &sql[i..i + 1],
            token_type: TokenType::Dot,
          });
          i += 1;
        } else {
          let mut token_type = TokenType::Float;
          // Floating point that starts with a dot. eg .0234
          let prev = i;
          i += 1;
          while i < n {
            if !sql_bytes[i].is_ascii_digit() {
              if sql_bytes[i] == SQLITE_DIGIT_SEPARATOR {
                token_type = TokenType::QNumber;
              } else {
                break;
              }
            }
            i += 1;
          }

          if matches!(sql_bytes[i], b'e' | b'E')
            && (sql_bytes[i + 1].is_ascii_digit()
              || (matches!(sql_bytes[i + 1], b'+' | b'-')) && sql_bytes[i + 2].is_ascii_digit())
          {
            i += 2;
            while i < n {
              if !sql_bytes[i].is_ascii_digit() {
                if sql_bytes[i] == SQLITE_DIGIT_SEPARATOR {
                  token_type = TokenType::QNumber;
                } else {
                  break;
                }
              }
              i += 1;
            }
          }

          while i < n && is_id_char(sql_bytes[i]) {
            token_type = TokenType::Illegal;
            i += 1;
          }

          tokens.push(Token {
            text: &sql[prev..i],
            token_type,
          });
        }
      }
      CharacterClasses::Digit => {
        let mut token_type = TokenType::Integer;
        let prev = i;
        if sql_bytes[i] == b'0'
          && (sql_bytes[i + 1] == b'x' || sql_bytes[i + 1] == b'X')
          && sql_bytes[i + 2].is_ascii_hexdigit()
        {
          // Hex integer
          i += 3;
          while i < n {
            if !sql_bytes[i].is_ascii_hexdigit() {
              if sql_bytes[i] == SQLITE_DIGIT_SEPARATOR {
                token_type = TokenType::QNumber;
              } else {
                break;
              }
            }
            i += 1;
          }
        } else {
          // Integer
          while i < n {
            if !sql_bytes[i].is_ascii_digit() {
              if sql_bytes[i] == SQLITE_DIGIT_SEPARATOR {
                token_type = TokenType::QNumber;
              } else {
                break;
              }
            }
            i += 1;
          }

          // Float
          // Same code as CharacterClasses::Dot float part above
          if sql_bytes[i] == b'.' {
            i += 1;
            while i < n {
              if !sql_bytes[i].is_ascii_digit() {
                if sql_bytes[i] == SQLITE_DIGIT_SEPARATOR {
                  token_type = TokenType::QNumber;
                } else {
                  break;
                }
              }
              i += 1;
            }
          }

          // Exponent
          if matches!(sql_bytes[i], b'e' | b'E')
            && (sql_bytes[i + 1].is_ascii_digit()
              || (matches!(sql_bytes[i + 1], b'+' | b'-')) && sql_bytes[i + 2].is_ascii_digit())
          {
            i += 2;
            while i < n {
              if !sql_bytes[i].is_ascii_digit() {
                if sql_bytes[i] == SQLITE_DIGIT_SEPARATOR {
                  token_type = TokenType::QNumber;
                } else {
                  break;
                }
              }
              i += 1;
            }
          }
        }
        while i < n && is_id_char(sql_bytes[i]) {
          token_type = TokenType::Illegal;
          i += 1;
        }

        tokens.push(Token {
          text: &sql[prev..i],
          token_type,
        });
      }
      CharacterClasses::Quote2 => {
        let prev = i;
        let mut c = sql_bytes[i];
        while c != b']' && i + 1 < n {
          c = sql_bytes[i + 1];
          i += 1;
        }
        let token_type = if c == b']' {
          TokenType::Id
        } else {
          TokenType::Illegal
        };
        tokens.push(Token {
          text: &sql[prev..i],
          token_type,
        });
      }
      CharacterClasses::VarNum => {
        let token_type = TokenType::Variable;
        let prev = i;
        while i < n && sql_bytes[i].is_ascii_digit() {
          i += 1;
        }
        tokens.push(Token {
          text: &sql[prev..i],
          token_type,
        });
      }
      CharacterClasses::Dollar | CharacterClasses::VarAlpha => {
        let mut token_type = TokenType::Variable;
        let prev = 0;
        let mut c = sql_bytes[i];
        let mut nn = 0;
        i += 1;
        // Act like SQLITE_OMIT_TCL_VARIABLE is always set
        while i < n {
          if is_id_char(c) {
            nn += 1;
          } else {
            break;
          }
          c = sql_bytes[i];
          i += 1;
        }

        if nn == 0 {
          token_type = TokenType::Illegal;
        }

        tokens.push(Token {
          text: &sql[prev..i],
          token_type,
        });
      }
      CharacterClasses::Kywd0 => {
        if AI_CLASS[sql_bytes[i + 1] as usize] > CharacterClasses::Kywd0 as u8 {
          let prev = i;
          i += 1;
          while i < n && is_id_char(sql_bytes[i]) {
            i += 1;
          }
          tokens.push(Token {
            text: &sql[prev..i],
            token_type: TokenType::Id,
          });
        } else {
          let prev = i;
          i += 2;
          while i < n && AI_CLASS[sql_bytes[i] as usize] <= CharacterClasses::Kywd0 as u8 {
            i += 1;
          }

          if i < n && is_id_char(sql_bytes[i]) {
            i += 1;

            while i < n && is_id_char(sql_bytes[i]) {
              i += 1;
            }
            tokens.push(Token {
              text: &sql[prev..i],
              token_type: TokenType::Id,
            });
          } else {
            // Either TokenType::Id or TokenType::{keyword}
            tokens.push(Token {
              text: &sql[prev..i],
              token_type: get_keyword_token_type(&sql[prev..i]),
            });
          }
        }
      }
      CharacterClasses::X => {
        let prev = i;
        if sql_bytes[i + 1] == b'\'' {
          let mut token_type = TokenType::Blob;
          i += 2;
          while i < n && sql_bytes[i].is_ascii_hexdigit() {
            i += 1;
          }

          // We didn't find the ending ' or number of bytes is odd
          if i < n && (sql_bytes[i] != b'\'' || (i - prev) % 2 == 1) {
            token_type = TokenType::Illegal;
            while i < n && sql_bytes[i] != b'\'' {
              i += 1;
            }
          }

          if i < n {
            i += 1;
          }

          tokens.push(Token {
            text: &sql[prev..i],
            token_type,
          });
        } else {
          i += 1;
          while i < n && is_id_char(sql_bytes[i]) {
            i += 1;
          }
          tokens.push(Token {
            text: &sql[prev..i],
            token_type: TokenType::Id,
          });
        }
      }
      CharacterClasses::Kywd | CharacterClasses::Id => {
        let prev = i;
        i += 1;
        while i < n && is_id_char(sql_bytes[i]) {
          i += 1;
        }
        tokens.push(Token {
          text: &sql[prev..i],
          token_type: TokenType::Id,
        });
      }
      CharacterClasses::Bom => {
        if sql_bytes[i + 1] == 0xbb && sql_bytes[i + 2] == 0xbf {
          tokens.push(Token {
            text: &sql[i..i + 3],
            token_type: TokenType::Space,
          });
          i += 3;
        } else {
          let prev = i;
          i += 1;
          while i < n && is_id_char(sql_bytes[i]) {
            i += 1;
          }
          tokens.push(Token {
            text: &sql[prev..i],
            token_type: TokenType::Id,
          });
        }
      }
      CharacterClasses::Nul => {
        // Take a closer look
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Illegal,
        });
        i += 1;
      }
      _ => {
        tokens.push(Token {
          text: &sql[i..i + 1],
          token_type: TokenType::Illegal,
        });
        i += 1;
      }
    }
  }

  tokens
}

fn get_keyword_token_type(text: &str) -> TokenType {
  match text.to_ascii_uppercase().as_str() {
    "ABORT" => TokenType::ABORT,
    "ACTION" => TokenType::ACTION,
    "ADD" => TokenType::ADD,
    "AFTER" => TokenType::AFTER,
    "ALL" => TokenType::ALL,
    "ALTER" => TokenType::ALTER,
    "ALWAYS" => TokenType::ALWAYS,
    "ANALYZE" => TokenType::ANALYZE,
    "AND" => TokenType::AND,
    "AS" => TokenType::AS,
    "ASC" => TokenType::ASC,
    "ATTACH" => TokenType::ATTACH,
    "AUTOINCREMENT" => TokenType::AUTOINCREMENT,
    "BEFORE" => TokenType::BEFORE,
    "BEGIN" => TokenType::BEGIN,
    "BETWEEN" => TokenType::BETWEEN,
    "BY" => TokenType::BY,
    "CASCADE" => TokenType::CASCADE,
    "CASE" => TokenType::CASE,
    "CAST" => TokenType::CAST,
    "CHECK" => TokenType::CHECK,
    "COLLATE" => TokenType::COLLATE,
    "COLUMN" => TokenType::COLUMN,
    "COMMIT" => TokenType::COMMIT,
    "CONFLICT" => TokenType::CONFLICT,
    "CONSTRAINT" => TokenType::CONSTRAINT,
    "CREATE" => TokenType::CREATE,
    "CROSS" => TokenType::CROSS,
    "CURRENT" => TokenType::CURRENT,
    "CURRENT_DATE" => TokenType::CURRENT_DATE,
    "CURRENT_TIME" => TokenType::CURRENT_TIME,
    "CURRENT_TIMESTAMP" => TokenType::CURRENT_TIMESTAMP,
    "DATABASE" => TokenType::DATABASE,
    "DEFAULT" => TokenType::DEFAULT,
    "DEFERRABLE" => TokenType::DEFERRABLE,
    "DEFERRED" => TokenType::DEFERRED,
    "DELETE" => TokenType::DELETE,
    "DESC" => TokenType::DESC,
    "DETACH" => TokenType::DETACH,
    "DISTINCT" => TokenType::DISTINCT,
    "DO" => TokenType::DO,
    "DROP" => TokenType::DROP,
    "EACH" => TokenType::EACH,
    "ELSE" => TokenType::ELSE,
    "END" => TokenType::END,
    "ESCAPE" => TokenType::ESCAPE,
    "EXCEPT" => TokenType::EXCEPT,
    "EXCLUDE" => TokenType::EXCLUDE,
    "EXCLUSIVE" => TokenType::EXCLUSIVE,
    "EXISTS" => TokenType::EXISTS,
    "EXPLAIN" => TokenType::EXPLAIN,
    "FAIL" => TokenType::FAIL,
    "FILTER" => TokenType::FILTER,
    "FIRST" => TokenType::FIRST,
    "FOLLOWING" => TokenType::FOLLOWING,
    "FOR" => TokenType::FOR,
    "FOREIGN" => TokenType::FOREIGN,
    "FROM" => TokenType::FROM,
    "FULL" => TokenType::FULL,
    "GENERATED" => TokenType::GENERATED,
    "GLOB" => TokenType::GLOB,
    "GROUP" => TokenType::GROUP,
    "GROUPS" => TokenType::GROUPS,
    "HAVING" => TokenType::HAVING,
    "IF" => TokenType::IF,
    "IGNORE" => TokenType::IGNORE,
    "IMMEDIATE" => TokenType::IMMEDIATE,
    "IN" => TokenType::IN,
    "INDEX" => TokenType::INDEX,
    "INDEXED" => TokenType::INDEXED,
    "INITIALLY" => TokenType::INITIALLY,
    "INNER" => TokenType::INNER,
    "INSERT" => TokenType::INSERT,
    "INSTEAD" => TokenType::INSTEAD,
    "INTERSECT" => TokenType::INTERSECT,
    "INTO" => TokenType::INTO,
    "IS" => TokenType::IS,
    "ISNULL" => TokenType::ISNULL,
    "JOIN" => TokenType::JOIN,
    "KEY" => TokenType::KEY,
    "LAST" => TokenType::LAST,
    "LEFT" => TokenType::LEFT,
    "LIKE" => TokenType::LIKE,
    "LIMIT" => TokenType::LIMIT,
    "MATCH" => TokenType::MATCH,
    "MATERIALIZED" => TokenType::MATERIALIZED,
    "NATURAL" => TokenType::NATURAL,
    "NO" => TokenType::NO,
    "NOT" => TokenType::NOT,
    "NOTHING" => TokenType::NOTHING,
    "NOTNULL" => TokenType::NOTNULL,
    "NULL" => TokenType::NULL,
    "NULLS" => TokenType::NULLS,
    "OF" => TokenType::OF,
    "OFFSET" => TokenType::OFFSET,
    "ON" => TokenType::ON,
    "OR" => TokenType::OR,
    "ORDER" => TokenType::ORDER,
    "OTHERS" => TokenType::OTHERS,
    "OUTER" => TokenType::OUTER,
    "OVER" => TokenType::OVER,
    "PARTITION" => TokenType::PARTITION,
    "PLAN" => TokenType::PLAN,
    "PRAGMA" => TokenType::PRAGMA,
    "PRECEDING" => TokenType::PRECEDING,
    "PRIMARY" => TokenType::PRIMARY,
    "QUERY" => TokenType::QUERY,
    "RAISE" => TokenType::RAISE,
    "RANGE" => TokenType::RANGE,
    "RECURSIVE" => TokenType::RECURSIVE,
    "REFERENCES" => TokenType::REFERENCES,
    "REGEXP" => TokenType::REGEXP,
    "REINDEX" => TokenType::REINDEX,
    "RELEASE" => TokenType::RELEASE,
    "RENAME" => TokenType::RENAME,
    "REPLACE" => TokenType::REPLACE,
    "RESTRICT" => TokenType::RESTRICT,
    "RETURNING" => TokenType::RETURNING,
    "RIGHT" => TokenType::RIGHT,
    "ROLLBACK" => TokenType::ROLLBACK,
    "ROW" => TokenType::ROW,
    "ROWS" => TokenType::ROWS,
    "SAVEPOINT" => TokenType::SAVEPOINT,
    "SELECT" => TokenType::SELECT,
    "SET" => TokenType::SET,
    "TABLE" => TokenType::TABLE,
    "TEMP" => TokenType::TEMP,
    "TEMPORARY" => TokenType::TEMPORARY,
    "THEN" => TokenType::THEN,
    "TIES" => TokenType::TIES,
    "TO" => TokenType::TO,
    "TRANSACTION" => TokenType::TRANSACTION,
    "TRIGGER" => TokenType::TRIGGER,
    "UNBOUNDED" => TokenType::UNBOUNDED,
    "UNION" => TokenType::UNION,
    "UNIQUE" => TokenType::UNIQUE,
    "UPDATE" => TokenType::UPDATE,
    "WHERE" => TokenType::WHERE,
    _ => TokenType::Id,
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn test_tokenizer() {
    let tokens = tokenize("SELECT * FROM t1;");
    let expected = [
      Token {
        text: "SELECT",
        token_type: TokenType::SELECT,
      },
      Token {
        text: " ",
        token_type: TokenType::Space,
      },
      Token {
        text: "*",
        token_type: TokenType::Star,
      },
      Token {
        text: " ",
        token_type: TokenType::Space,
      },
      Token {
        text: "FROM",
        token_type: TokenType::FROM,
      },
      Token {
        text: " ",
        token_type: TokenType::Space,
      },
      Token {
        text: "t1",
        token_type: TokenType::Id,
      },
      Token {
        text: ";",
        token_type: TokenType::Semi,
      },
    ];

    assert_eq!(tokens, expected);
  }
}
