use crate::{Connection, Row, error::KleinDBError};
use std::{
  env,
  io::{self, BufRead, BufReader, Write},
  path::Path,
};

const MAIN_PROMPT: &str = "kleindb> ";
const CONTINUATION_PROMPT: &str = "    ...> ";

/// Shell configuration and state
struct Shell<R: BufRead, W: Write> {
  conn: Option<Connection>,
  reader: R,
  writer: W,
  line_number: usize,
}

impl<R: BufRead, W: Write> Shell<R, W> {
  fn new(reader: R, writer: W) -> Self {
    Self {
      conn: None,
      reader,
      writer,
      line_number: 0,
    }
  }

  /// Open or create a database connection
  fn open_database(&mut self, path: &str) -> Result<(), KleinDBError> {
    self.conn = Some(Connection::open(path)?);
    Ok(())
  }

  /// Ensure database is open, opening it if necessary
  fn ensure_open(&mut self, db_path: &str) -> Result<(), KleinDBError> {
    if self.conn.is_none() {
      self.open_database(db_path)?;
    }
    Ok(())
  }

  /// Run the interactive REPL
  fn run(&mut self, db_path: String) -> Result<(), KleinDBError> {
    // Open database if file exists (avoid creating empty files on typos)
    if Path::new(&db_path).exists() || db_path == ":memory:" {
      self.open_database(&db_path)?;
    }

    let mut sql_buffer = String::new();
    let mut start_line = 0;

    loop {
      let is_continuation = !sql_buffer.is_empty();

      match self.read_line(is_continuation)? {
        Some(line) => {
          self.line_number += 1;

          if sql_buffer.is_empty() {
            start_line = self.line_number;
            sql_buffer = line;
          } else {
            sql_buffer.push('\n');
            sql_buffer.push_str(&line);
          }

          // Execute if we have a complete statement
          if !sql_buffer.trim().is_empty() && is_complete_sql(&sql_buffer) {
            self.ensure_open(&db_path)?;

            if let Err(e) = self.execute_sql(&sql_buffer, start_line) {
              writeln!(self.writer, "Error at line {}: {}", start_line, e)?;
            }

            sql_buffer.clear();
          }
        }
        None => break, // EOF
      }
    }

    Ok(())
  }

  /// Read a single line of input
  fn read_line(&mut self, is_continuation: bool) -> Result<Option<String>, KleinDBError> {
    let prompt = if is_continuation {
      CONTINUATION_PROMPT
    } else {
      MAIN_PROMPT
    };

    write!(self.writer, "{}", prompt)?;
    self.writer.flush()?;

    let mut line = String::new();
    match self.reader.read_line(&mut line) {
      Ok(0) => Ok(None), // EOF
      Ok(_) => Ok(Some(line)),
      Err(e) => Err(KleinDBError::Io(e)),
    }
  }

  /// Execute a SQL statement and display results
  fn execute_sql(&mut self, sql: &str, _start_line: usize) -> Result<(), KleinDBError> {
    let conn = self.conn.as_ref().ok_or(KleinDBError::NoDatabase)?;

    // Try to execute as a query first (SELECT statements)
    match conn.query(sql) {
      Ok(rows) => {
        self.display_results(rows)?;
        Ok(())
      }
      Err(_) => {
        // If query fails, try as an execute (INSERT, UPDATE, DELETE, etc.)
        conn.execute(sql)?;
        writeln!(self.writer, "Done")?;
        Ok(())
      }
    }
  }

  /// Display query results in a simple table format
  fn display_results(
    &mut self,
    rows: impl Iterator<Item = Result<Row, KleinDBError>>,
  ) -> Result<(), KleinDBError> {
    let mut row_count = 0;

    for row_result in rows {
      let row = row_result?;

      // Display the row columns separated by " | "
      for (i, value) in row.columns().iter().enumerate() {
        write!(self.writer, "{}", value)?;
        if i < row.columns().len() - 1 {
          write!(self.writer, " | ")?;
        }
      }
      writeln!(self.writer)?;
      row_count += 1;
    }

    if row_count == 0 {
      writeln!(self.writer, "(no rows)")?;
    }

    self.writer.flush()?;
    Ok(())
  }
}

/// Check if SQL statement is complete (ends with semicolon outside quotes/comments)
fn is_complete_sql(sql: &str) -> bool {
  #[derive(Clone, Copy)]
  enum State {
    Normal,
    AfterSemicolon,
    InString(u8), // Store the quote character
  }

  let mut state = State::Normal;

  for &byte in sql.as_bytes() {
    state = match (state, byte) {
      // Handle string literals
      (State::Normal, quote @ (b'"' | b'\'' | b'`')) => State::InString(quote),
      (State::InString(quote), b) if b == quote => State::Normal,
      (State::InString(quote), _) => State::InString(quote),

      // Handle semicolons
      (State::Normal, b';') => State::AfterSemicolon,
      (State::AfterSemicolon, b) if b.is_ascii_whitespace() => State::AfterSemicolon,
      (State::AfterSemicolon, _) => State::Normal,

      // Everything else
      (state, _) => state,
    };
  }

  matches!(state, State::AfterSemicolon)
}

pub fn run() -> Result<(), KleinDBError> {
  let args: Vec<String> = env::args().collect();

  let db_path = args
    .get(1)
    .filter(|arg| !arg.starts_with('-'))
    .cloned()
    .unwrap_or_else(|| {
      #[cfg(not(feature = "omit_memorydb"))]
      {
        ":memory:".to_string()
      }
      #[cfg(feature = "omit_memorydb")]
      {
        eprintln!("Error: no database filename specified");
        std::process::exit(1);
      }
    });

  let stdin = BufReader::new(io::stdin());
  let stdout = io::stdout();

  let mut shell = Shell::new(stdin, stdout);
  shell.run(db_path)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_sql_completion() {
    assert!(is_complete_sql("SELECT * FROM users;"));
    assert!(is_complete_sql("SELECT * FROM users; "));
    assert!(is_complete_sql("SELECT * FROM users;\n"));
    assert!(!is_complete_sql("SELECT * FROM"));
    assert!(!is_complete_sql("SELECT * FROM users"));

    // Handle strings with semicolons
    assert!(!is_complete_sql("SELECT 'hello;' FROM"));
    assert!(is_complete_sql("SELECT 'hello;' FROM users;"));
  }
}
