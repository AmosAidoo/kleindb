#[derive(Debug, Clone)]
pub enum MemValue {
  Undefined,
  Integer(i32),
  Real(f64),
  String(String),
}

/// These are Mems
#[derive(Debug, Clone)]
pub struct SQLite3Value {
  pub value: MemValue,
}
