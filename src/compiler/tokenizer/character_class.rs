// Character classes for tokenizing
pub enum CharacterClass {
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

impl From<u8> for CharacterClass {
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
pub const AI_CLASS: [u8; 256] = [
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
