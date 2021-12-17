use std::{borrow::Cow, fmt::Display};

use crate::{errors::Result, read_vint, writable::Writable};

/// hadoop.io.Text
/// warning -- utf8 special is not implemented
/// will deliver proper results only if underlying string is 'simple' enough
#[derive(Debug)]
pub struct Text {
    len: i32,
    buf: Vec<u8>,
}

impl Text {
    /// Converts to String
    pub fn to_string(&self) -> Cow<str> {
        String::from_utf8_lossy(&self.buf)
    }

    /// Tells if instance is an empty string or not
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Writable for Text {
    fn read(input: &mut impl std::io::Read) -> Result<Self> {
        let len = read_vint(input)?;
        let mut buf = vec![0; len as usize];
        input.read_exact(&mut buf)?;
        Ok(Self { len, buf })
    }
}

impl Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
