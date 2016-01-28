use std::{io, str, error, fmt, result};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BadMagic(String),
    VersionNotSupported(u16),
    CompressionTypeUnknown(String),
    SyncMarkerMismatch,
    IO(io::Error),
    BadEncoding(str::Utf8Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match *self {
            Error::BadMagic(ref m) => {
                write!(f,
                       "Bad or missing magic string, found: '{}'. Are you sure this is a sequence \
                        file?'",
                       m)
            }
            Error::IO(_) => write!(f, "I/O Error: {}", self),
            Error::VersionNotSupported(ref v) => write!(f, "Unexpected version: '{}'", v),
            Error::SyncMarkerMismatch => write!(f, "Sync marker mismatch"),
            Error::CompressionTypeUnknown(ref codec) => {
                write!(f, "Unexpected compression codec: '{}'", codec)
            }
            Error::BadEncoding(ref e) => write!(f, "UTF8 Error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::BadMagic(_) => "Bad or missing magic header.",
            Error::VersionNotSupported(_) => "Bad version number.",
            Error::SyncMarkerMismatch => "Sync marker mismatch",
            Error::CompressionTypeUnknown(_) => "Unable to decompress, unknown codec.",
            Error::IO(ref e) => e.description(),
            Error::BadEncoding(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IO(ref e) => Some(e),
            Error::BadEncoding(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<str::Utf8Error> for Error {
    fn from(err: str::Utf8Error) -> Error {
        Error::BadEncoding(err)
    }
}
