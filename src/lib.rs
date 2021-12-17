//! Prototype streaming library for reading Hadoop sequencefiles
//!
//! # Example
//! ```ignore
//! let path = Path::new("/path/to/seqfile");
//! let file = File::open(&path).unwrap();
//!
//! let seqfile = match sequencefile::Reader::new(file) {
//!   Ok(val) => val,
//!   Err(err) => panic!("Failed to open sequence file: {}", err),
//! };
//!
//! // Returns a Result<(ByteString, ByteString)>, where a ByteString is a Vec<u8>
//! // An Err from this will signal an unrecoverable error. Next call to Iterator
//! // Returns None
//! for kv in seqfile {
//!     println!("{:?}", kv);
//! }
//! ```

#![deny(
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces
)]

extern crate byteorder;
extern crate bzip2;
extern crate flate2;

use std::collections::HashMap;

/// Convenience typedef
pub type ByteString = Vec<u8>;

/// Sequencefile header, metadata about the file, e.g. key/value types, version, compression
/// and some internal state for properly decoding
#[derive(Debug)]
pub struct Header {
    /// Sequencefile version
    /// Version 4 - block compression
    /// Version 5 - custom compression codecs
    /// Version 6 - metadata
    pub version: u16,

    /// Type of value compression
    pub compression_type: CompressionType,

    /// Codec, if any
    pub compression_codec: Option<Codec>,

    /// Fully-qualified Java class of key Writable
    pub key_class: String,

    /// Fully-qualified Java class of value Writable
    pub value_class: String,

    /// K-V metadata on sequencefile
    pub metadata: HashMap<String, String>,
    sync_marker: ByteString,
}

// modules
mod compress;
mod text;
mod util;

/// Error
pub mod errors;
pub mod reader;
/// writable trait and some implementations
pub mod writable;

// exports
pub use compress::{Codec, CompressionType};
pub use errors::*;
pub use reader::*;
pub use text::*;

#[cfg(test)]
pub mod tests;
