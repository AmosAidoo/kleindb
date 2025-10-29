use std::{
  env,
  io::{BufRead, BufReader, Write},
  path::Path,
  sync::{Arc, Mutex},
};

use kleindb::{
  KleinDBContext, SQLite3, StepStatus, compiler::prepare::sqlite3_prepare_v2, is_id_char,
};

const MAIN_PROMPT: &str = "kleindb> ";
const CONTINUATION_PROMPT: &str = "   ...> ";

#[allow(dead_code)]
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

/// Storage space for auxiliary database connection
struct AuxDb {
  db_filename: String,
}

struct ShellState<'a> {
  /// Kleindb Context
  ctx: &'a mut KleinDBContext,

  /// Current statement if any
  // p_stmt: &'a SQLite3Stmt,

  /// Read commands from this stream
  inp: Box<dyn BufRead + 'static>,

  /// Write results here
  out: Box<dyn Write + 'static>,

  #[allow(dead_code)]
  open_mode: ShellOpenModes,

  /// Line number of last line read from in
  lineno: usize,

  a_aux_db: [AuxDb; 5],
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
    // Accumulated SQL text
    let mut z_sql: String = String::new();
    // Length of current line
    let mut n_line: usize = 0;
    // Bytes of zSql[] used
    let mut n_sql: usize = 0;
    let mut startline: usize = 0;

    // I have left out all the codes that have got to do with
    // QuickScanState. I believe those are optimization to avoid
    // certain extra work. I don't understand it 100% but I also know
    // I probably don't need that kind of optimization now

    self.lineno = 0;
    // There were 2 more conditions in sqlite3
    // !bail_on_error || (p->in==0 && stdin_is_interactive)
    // err_count == 0
    loop {
      let z_line = self.one_input_line(n_sql > 0);
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

  /// Make sure the database is open.  If it is not, then open it.  If
  /// the database fails to open, panic
  fn open_db(&mut self) {
    let db_filename = &self.a_aux_db[0].db_filename;

    // TODO: Handle all the open modes
    let _ = self.ctx.sqlite3_open_v2(db_filename);

    // TODO: Initialize/load functions
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
  fn run_one_sql_line(&mut self, z_sql: &str, _startline: usize) {
    self.open_db();
    let _ = self.shell_exec(z_sql);
  }

  /// Execute a statement or set of statements.
  fn shell_exec(&mut self, z_sql: &str) -> Result<(), ShellExecError> {
    let stmt = sqlite3_prepare_v2(self.ctx, z_sql);
    println!("{:?}", stmt.a_op);
    self.ctx.set_vdbe(stmt);
    self.exec_prepared_stmt();
    Ok(())
  }

  fn exec_prepared_stmt(&mut self) {
    let mut rc = self.ctx.sqlite3_step();
    let n_column = self.ctx.vdbe.as_ref().unwrap().sqlite3_column_count();

    if let Ok(status) = rc.as_mut() {
      println!("status: {:?}, n_column: {}", status, n_column);
      if matches!(status, StepStatus::Row) {
        while matches!(status, StepStatus::Row) {
          let p_stmt = self.ctx.vdbe.as_ref().unwrap();
          for i in 0..n_column {
            let buf = p_stmt.sqlite3_column_text(i).bytes().collect::<Vec<_>>();
            let _ = self.out.write_all(&buf);
            if i != n_column - 1 {
              let _ = self.out.write_all(b" | ");
            }
          }
          let _ = self.out.write_all(b"\n");
          let _ = self.out.flush();
          *status = self.ctx.sqlite3_step().unwrap();
        }
      }
    }
  }
}

fn main() {
  // let warn_in_memory_db = false;
  // let read_stdin = true;
  let mut ctx = KleinDBContext {
    db: Arc::new(Mutex::new(SQLite3::new())),
    vdbe: None,
  };
  // let p_stmt = SQLite3Stmt {};

  let a_aux_db: [AuxDb; 5] = [
    AuxDb {
      db_filename: String::new(),
    },
    AuxDb {
      db_filename: String::new(),
    },
    AuxDb {
      db_filename: String::new(),
    },
    AuxDb {
      db_filename: String::new(),
    },
    AuxDb {
      db_filename: String::new(),
    },
  ];
  let mut shell_state = ShellState {
    ctx: &mut ctx,
    // p_stmt: &p_stmt,
    inp: Box::new(BufReader::new(std::io::stdin())),
    out: Box::new(std::io::stdout()),
    open_mode: ShellOpenModes::Unspec,
    lineno: 0,
    a_aux_db,
  };

  let mut args = env::args();
  args.next();

  for arg in args {
    println!("{arg}");
    if !arg.starts_with('-') {
      // This is the name of the database
      if shell_state.a_aux_db[0].db_filename.is_empty() {
        shell_state.a_aux_db[0].db_filename = arg;
      }
    }
  }

  if shell_state.a_aux_db[0].db_filename.is_empty() {
    if !cfg!(feature = "omit_memorydb") {
      shell_state.a_aux_db[0].db_filename = ":memory:".to_string();
    } else {
      panic!("Error: no database filename spefified");
    }
  }

  // Go ahead and open the database file if it already exists.  If the
  // file does not exist, delay opening it.  This prevents empty database
  // files from being created if a user mistypes the database name argument
  // to the sqlite command-line tool.
  let path = Path::new(&shell_state.a_aux_db[0].db_filename);
  let exists = path.try_exists().expect("Can't check existence of file");

  if exists {
    shell_state.open_db();
  }

  shell_state.process_input();
}
