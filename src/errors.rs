use byteorder;
use std::{
    error,
    fmt::{self},
    io, result, str,
};

/// convenience result type
pub type Result<T> = result::Result<T, Error>;

/// Error enum
#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum Error {
    /// bad magic sequence
    BadMagic(String),
    /// sequence file version not supported
    VersionNotSupported(u16),
    /// unknown sequence type
    CompressionTypeUnknown(String),
    /// unsupported coded
    UnsupportedCodec(String),
    /// marker mismatch (file corrupted)
    SyncMarkerMismatch,
    /// end of file
    EOF,
    /// some IO error
    IO(io::Error),
    /// unsupported encoding
    BadEncoding(str::Utf8Error),
    /// unexpected decoder
    UnexpectedDecoder(byteorder::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match &*self {
            Error::BadMagic(ref m) => {
                write!(
                    f,
                    "bad or missing magic string, found: '{}'. Is this a sequence file?",
                    m
                )
            }
            Error::IO(io) => write!(f, "i/o error: {}", io),
            Error::VersionNotSupported(ref v) => write!(f, "unexpected version: '{}'", v),
            Error::SyncMarkerMismatch => write!(f, "sync marker mismatch"),
            Error::EOF => write!(f, "end of file"),
            Error::CompressionTypeUnknown(ref codec) => {
                write!(f, "unexpected compression type: '{}'", codec)
            }
            Error::UnsupportedCodec(ref codec) => write!(f, "unsupported codec: '{}'", codec),
            Error::BadEncoding(ref e) => write!(f, "utf8 error: {}", e),
            Error::UnexpectedDecoder(ref e) => write!(f, "decoding error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::IO(ref e) => Some(e),
            Error::BadEncoding(ref e) => Some(e),
            Error::UnexpectedDecoder(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<byteorder::Error> for Error {
    fn from(err: byteorder::Error) -> Error {
        Error::UnexpectedDecoder(err)
    }
}

impl From<str::Utf8Error> for Error {
    fn from(err: str::Utf8Error) -> Error {
        Error::BadEncoding(err)
    }
}
