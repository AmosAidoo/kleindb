use std::{
  io::{BufRead, BufReader, Write},
  sync::{Arc, Mutex},
};

use kleindb::{
  KleinDBContext, SQLite3, SQLite3Stmt, compiler::prepare::sqlite3_prepare_v2, is_id_char,
};

const MAIN_PROMPT: &str = "sqlite> ";
const CONTINUATION_PROMPT: &str = "   ...> ";

enum ShellOpenModes {
  /// No open-mode specified
  Unspec,
  /// Normal database file
  Normal,
  /// Use appendvfs
  AppendVFS,
  /// Use the zipfile virtual table
  Zipfile,
  /// Open a normal database read-only
  ReadOnly,
  /// Open using sqlite3_deserialize()
  Deserialize,
  /// Use "dbtotxt" output as data source
  HexDB,
}

struct ShellState<'a> {
  /// Kleindb Context
  ctx: &'a KleinDBContext,

  /// Current statement if any
  // p_stmt: &'a SQLite3Stmt,

  /// Read commands from this stream
  inp: Box<dyn BufRead + 'static>,

  /// Write results here
  out: Box<dyn Write + 'static>,

  open_mode: ShellOpenModes,

  /// Line number of last line read from in
  lineno: usize,
}

// Token types used by the sqlite3_complete() routine.
const TK_SEMI: u8 = 0;
const TK_WS: u8 = 1;
const TK_OTHER: u8 = 2;

/// Return true if the given SQL string ends in a semicolon.
fn sqlite3_complete(z_sql: &str) -> bool {
  let mut state: u8 = 0;

  // Transitions between states
  const TRANS: [[u8; 3]; 3] = [[1, 0, 2], [1, 1, 2], [1, 2, 2]];
  let sql_bytes: Vec<u8> = z_sql.bytes().collect();
  let mut i: usize = 0;
  while i < sql_bytes.len() {
    let token = match sql_bytes[i] {
      // A semicolon
      b';' => TK_SEMI,
      // White space is ignored
      b' ' | b'\r' | b'\t' | b'\n' | 0x0c => TK_WS,
      // Left out C-style comments
      // Left out SQL-style comments
      // Left out Microsoft-style identifiers in [...]
      b'`' | b'"' | b'\'' => {
        let c = sql_bytes[i];
        i += 1;
        while i < sql_bytes.len() && sql_bytes[i] != c {
          i += 1;
        }
        if i >= sql_bytes.len() {
          return false;
        }
        TK_OTHER
      }
      _ => {
        if is_id_char(sql_bytes[i]) {
          let mut n_id = 0;
          while n_id < sql_bytes.len() && is_id_char(sql_bytes[i + n_id]) {
            n_id += 1;
          }
          // TODO: Use feature flags to support granular tokens like create, trigger, ...
          i += n_id - 1;
          TK_OTHER
        } else {
          TK_OTHER
        }
      }
    };
    state = TRANS[state as usize][token as usize];
    i += 1;
  }

  state == 1
}

enum ShellExecError {}

impl ShellState<'_> {
  fn process_input(&mut self) {
    // A single input line
    let mut z_line: Option<String> = None;
    // Accumulated SQL text
    let mut z_sql: String = String::new();
    // Length of current line
    let mut n_line: usize = 0;
    // Bytes of zSql[] used
    let mut n_sql: usize = 0;
    let mut err_cnt = 0;
    let mut startline: usize = 0;

    // I have left out all the codes that have got to do with
    // QuickScanState. I believe those are optimization to avoid
    // certain extra work. I don't understand it 100% but I also know
    // I probably don't need that kind of optimization now

    self.lineno = 0;
    // There were 2 more conditions in sqlite3
    // !bail_on_error || (p->in==0 && stdin_is_interactive)
    while err_cnt == 0 {
      z_line = self.one_input_line(n_sql > 0);
      if let Some(line) = z_line.as_ref() {
        self.lineno += 1;

        n_line += line.len();

        if n_sql == 0 {
          // Find first non-whitespace character
          let i = z_line
            .as_ref()
            .unwrap()
            .bytes()
            .position(|ch| !ch.is_ascii_whitespace())
            .unwrap();
          startline = self.lineno;
          z_sql.push_str(z_line.as_ref().unwrap());
          n_sql = n_line - i;
        } else {
          z_sql.push('\n');
          z_sql.push_str(z_line.as_ref().unwrap());
          n_sql += n_line;
        }

        // println!("{}, {}", n_sql, sqlite3_complete(&z_sql));
        if n_sql > 0 && sqlite3_complete(&z_sql) {
          let _ = self.out.write_all(z_sql.as_bytes());
          let _ = self.out.flush();
          n_sql = 0;
          // Try to execute sql
          self.run_one_sql_line(&z_sql, startline);
          z_sql.clear();
        }
      } else {
        break;
      }
    }
  }

  fn open_db(&self) {
    // In the sqlite code, there is a check to see if
    // db is stdin, I'm not doing that check at this point
  }

  fn one_input_line(&mut self, is_continuation: bool) -> Option<String> {
    let mut result = String::new();

    // Note: There is a check whether inp is stdin or not
    // in the original code
    let prompt = if is_continuation {
      CONTINUATION_PROMPT
    } else {
      MAIN_PROMPT
    };
    let prompt = prompt.bytes().collect::<Vec<_>>();
    let _ = self.out.write_all(&prompt);
    let _ = self.out.flush();

    let _ = self.inp.read_line(&mut result);

    Some(result)
  }

  /// Run a single line of SQL.  Return the number of errors.
  fn run_one_sql_line(&self, z_sql: &str, startline: usize) {
    let _ = self.shell_exec(z_sql);
  }

  /// Execute a statement or set of statements.
  fn shell_exec(&self, z_sql: &str) -> Result<(), ShellExecError> {
    sqlite3_prepare_v2(self.ctx, z_sql);
    Ok(())
  }
}

fn main() {
  // let warn_in_memory_db = false;
  // let read_stdin = true;

  let ctx = KleinDBContext {
    db: Arc::new(Mutex::new(SQLite3 {})),
  };
  // let p_stmt = SQLite3Stmt {};

  let mut shell_state = ShellState {
    ctx: &ctx,
    // p_stmt: &p_stmt,
    inp: Box::new(BufReader::new(std::io::stdin())),
    out: Box::new(std::io::stdout()),
    open_mode: ShellOpenModes::Unspec,
    lineno: 0,
  };

  shell_state.process_input();
}
