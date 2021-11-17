use crate::errors::Result;
use crate::{read_vint, Writable};

use std::borrow::Cow;
use std::io::Read;

/// hadoop.io.Text
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
    fn read(input: &mut [u8]) -> Result<Self> {
        let mut reader = std::io::Cursor::new(input);
        let len = read_vint(&mut reader)?;
        let mut buf = vec![0; len as usize];
        reader.read_exact(&mut buf)?;
        Ok(Self { len, buf })
    }
}
