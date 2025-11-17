use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
  core::{
    db::{Db, SQLite3},
    result::StepStatus,
  },
  error::KleinDBError,
  storage::btree::Btree,
  vdbe::stmt::SQLite3Stmt,
};

#[derive(Clone, Default)]
pub struct SharedDB(pub Arc<Mutex<SQLite3>>);

impl SharedDB {
  pub(crate) fn lock(&self) -> MutexGuard<'_, SQLite3> {
    self.0.lock().unwrap()
  }
}

#[derive(Default)]
pub struct Connection {
  db: SharedDB,
}

impl Connection {
  /// Open a database connection
  pub fn open(filename: &str) -> Result<Self, KleinDBError> {
    let db = SharedDB::default();
    {
      let mut guard = db.lock();

      guard.a_db.extend((0..2).map(|_| Db::default()));

      // TODO: Parse the filename/URI argument

      // Open the backend database driver
      guard.a_db[0].bt = Some(Btree::open(filename, &guard)?);
    }
    Ok(Self { db })
  }

  /// Execute a query that returns rows (SELECT)
  pub fn query(&self, sql: &str) -> Result<Rows, KleinDBError> {
    let guard = self.db.lock();
    let stmt = guard.sqlite3_prepare(sql);

    // Get column information before releasing the guard
    let column_count = stmt.sqlite3_column_count();
    let column_names: Vec<String> = (0..column_count)
      .map(|i| format!("col_{}", i)) // TODO: Get actual column names from stmt
      .collect();

    drop(guard);

    Ok(Rows {
      stmt: Some(stmt),
      column_names,
      finished: false,
    })
  }

  /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE)
  pub fn execute(&self, sql: &str) -> Result<(), KleinDBError> {
    let guard = self.db.lock();
    let mut stmt = guard.sqlite3_prepare(sql);
    stmt.sqlite3_step();
    Ok(())
  }

  // Prepare a statement for repeated execution
  pub fn prepare(&self, sql: &str) -> Result<Statement, KleinDBError> {
    let guard = self.db.lock();
    let stmt = guard.sqlite3_prepare(sql);
    drop(guard);

    Ok(Statement { stmt })
  }
}

/// Prepared statement for repeated execution
pub struct Statement {
  stmt: SQLite3Stmt,
}

impl Statement {
  /// Execute and return result rows
  pub fn query(&mut self) -> Result<Rows, KleinDBError> {
    let column_count = self.stmt.sqlite3_column_count();
    let column_names: Vec<String> = (0..column_count).map(|i| format!("col_{}", i)).collect();

    Ok(Rows {
      stmt: Some(self.stmt.clone()),
      column_names,
      finished: false,
    })
  }

  /// Execute and return nothing (for INSERT/UPDATE/DELETE)
  pub fn execute(&mut self) -> Result<(), KleinDBError> {
    self.stmt.sqlite3_step();
    Ok(())
  }
}

/// Iterator over query results
pub struct Rows {
  stmt: Option<SQLite3Stmt>,
  column_names: Vec<String>,
  finished: bool,
}

impl Rows {
  /// Get the number of columns
  pub fn column_count(&self) -> usize {
    self.column_names.len()
  }

  /// Get column names
  pub fn column_names(&self) -> &[String] {
    &self.column_names
  }
}

impl Iterator for Rows {
  type Item = Result<Row, KleinDBError>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.finished {
      return None;
    }

    let stmt = self.stmt.as_mut()?;

    match stmt.sqlite3_step() {
      StepStatus::Row => {
        let n_column = stmt.sqlite3_column_count();
        let columns: Vec<String> = (0..n_column).map(|i| stmt.sqlite3_column_text(i)).collect();

        Some(Ok(Row { columns }))
      }
      StepStatus::Done => {
        self.finished = true;
        None
      }
      StepStatus::Ok => {
        self.finished = true;
        None
      }
    }
  }
}

/// A single row of data
#[derive(Debug, Clone)]
pub struct Row {
  columns: Vec<String>,
}

impl Row {
  /// Get all columns as strings
  pub fn columns(&self) -> &[String] {
    &self.columns
  }

  /// Get a column value by index as a string
  pub fn get(&self, index: usize) -> Result<&str, KleinDBError> {
    self
      .columns
      .get(index)
      .map(|s| s.as_str())
      .ok_or(KleinDBError::InvalidColumn(index))
  }

  /// Get a column value by index with type conversion
  pub fn get_as<T: FromSql>(&self, index: usize) -> Result<T, KleinDBError> {
    let value = self.get(index)?;
    T::from_sql(value)
  }

  /// Get the number of columns
  pub fn len(&self) -> usize {
    self.columns.len()
  }

  /// Check if row is empty (has no columns)
  pub fn is_empty(&self) -> bool {
    self.columns.is_empty()
  }
}

/// Trait for types that can be extracted from SQL results
pub trait FromSql: Sized {
  fn from_sql(value: &str) -> Result<Self, KleinDBError>;
}

// Implement FromSql for common types
impl FromSql for String {
  fn from_sql(value: &str) -> Result<Self, KleinDBError> {
    Ok(value.to_string())
  }
}

impl FromSql for i32 {
  fn from_sql(value: &str) -> Result<Self, KleinDBError> {
    value.parse().map_err(|_| KleinDBError::TypeConversion {
      expected: "i32".to_string(),
      got: value.to_string(),
    })
  }
}

impl FromSql for i64 {
  fn from_sql(value: &str) -> Result<Self, KleinDBError> {
    value.parse().map_err(|_| KleinDBError::TypeConversion {
      expected: "i64".to_string(),
      got: value.to_string(),
    })
  }
}

impl FromSql for f64 {
  fn from_sql(value: &str) -> Result<Self, KleinDBError> {
    value.parse().map_err(|_| KleinDBError::TypeConversion {
      expected: "f64".to_string(),
      got: value.to_string(),
    })
  }
}

impl FromSql for bool {
  fn from_sql(value: &str) -> Result<Self, KleinDBError> {
    match value {
      "0" | "false" | "FALSE" | "False" => Ok(false),
      "1" | "true" | "TRUE" | "True" => Ok(true),
      _ => Err(KleinDBError::TypeConversion {
        expected: "bool".to_string(),
        got: value.to_string(),
      }),
    }
  }
}

impl<T: FromSql> FromSql for Option<T> {
  fn from_sql(value: &str) -> Result<Self, KleinDBError> {
    if value.is_empty() || value.eq_ignore_ascii_case("null") {
      Ok(None)
    } else {
      Ok(Some(T::from_sql(value)?))
    }
  }
}
