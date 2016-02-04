//! Implementation and structs for a sequencefile reader

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use std::io;
use std::io::prelude::*;
use std::collections::HashMap;
use std::str;
use errors::{Error, Result};
use {ByteString, Header};
use compress;
use compress::CompressionType;
use util::ZeroCompress;

const MAGIC: &'static str = "SEQ";
const SYNC_SIZE: usize = 16;

/// Provides a streaming interface fronted by an Iterator
/// Only buffers when `CompressionType::Block` is used.
#[derive(Debug)]
pub struct Reader<R: io::Read> {
    header: Header,
    reader: R,
    block_buffer: Vec<(ByteString, ByteString)>,
}

impl<R: io::Read> Reader<R> {
    /// Create a new Reader from an io::Read
    ///
    /// # Failures
    /// Returns an `Error` if sequencefile header is malformed, e.g. unsupported version or
    /// invalid compression algorithm
    pub fn new(mut r: R) -> Result<Reader<R>> {
        // TODO: handle this
        let header = try!(read_header(&mut r));

        Ok(Reader {
            header: header,
            reader: r,
            block_buffer: Vec::new(),
        })
    }
}

fn read_header<R: io::Read>(reader: &mut R) -> Result<Header> {
    let mut magic = [0; 3];
    try!(reader.read(&mut magic));
    if magic != MAGIC.as_bytes() {
        return Err(Error::BadMagic(String::from_utf8_lossy(&magic).to_string()));
    }

    let mut version = [0; 1];
    try!(reader.read(&mut version));
    let version = version[0] as u16;

    // Version 4 - block compression
    // Version 5 - custom compression codecs
    // Version 6 - metadata
    if version < 5 || version > 6 {
        return Err(Error::VersionNotSupported(version));
    }

    let key_class = try!(read_string(reader));
    let value_class = try!(read_string(reader));

    let mut flags = [0; 2];
    try!(reader.read(&mut flags));

    let compression_type: CompressionType = {
        // first byte: compression t/f
        // second byte: block t/f
        match (flags[0], flags[1]) {
            (1, 1) => CompressionType::Block,
            (1, 0) => CompressionType::Record,
            (0, 0) => CompressionType::None,
            _ => {
                return Err(Error::CompressionTypeUnknown("undefined compression type".to_string()))
            }
        }
    };

    let compression_codec = if compression_type != CompressionType::None {
        let codec = try!(read_string(reader));

        match compress::codec(&codec) {
            Some(codec) => Some(codec),
            None => return Err(Error::UnsupportedCodec(codec)),
        }
    } else {
        None
    };

    let pairs = try!(reader.read_u32::<BigEndian>());
    for _ in 0..pairs {
        // TODO: do stuff
    }

    let mut sync_marker = [0; SYNC_SIZE];
    try!(reader.read(&mut sync_marker));

    Ok(Header {
        version: version,
        compression_type: compression_type,
        compression_codec: compression_codec,
        key_class: key_class,
        value_class: value_class,
        metadata: HashMap::new(), // TODO
        sync_marker: sync_marker.to_vec(),
    })
}

impl<R: io::Read> Iterator for Reader<R> {
    type Item = (ByteString, ByteString);

    fn next(&mut self) -> Option<(ByteString, ByteString)> {
        if self.block_buffer.len() == 0 || self.header.compression_type != CompressionType::Block {
            let mut last_sync_marker = [0; SYNC_SIZE];
            let kv_length = to_opt!(self.reader.read_i32::<BigEndian>()) as i64;
            // ^ todo: Varint reader
            // let blocksize = to_opt!(self.reader.decode_vint64());
            println!("kv_length: {:?}", kv_length);
            // let mut kv_length = blocksize;

            // handle sync marker
            if kv_length == -1 {
                to_opt!(self.reader.read(&mut last_sync_marker));
                if last_sync_marker.to_vec() != self.header.sync_marker {
                    panic!("Sync marker mismatch");
                }

                // kv_length = to_opt!(self.reader.decode_vint64());
                //
                // if kv_length == -1 {
                //     return None;
                // }
            }


            if self.header.compression_type != CompressionType::Block {
                let (key, value) = to_opt!(read_kv(kv_length as isize,
                                                   &self.header,
                                                   &mut self.reader));

                return Some((key, value));
            }
        }

        if self.header.compression_type == CompressionType::Block {
            if self.block_buffer.len() == 0 {
                let kv_count = to_opt!(self.reader.decode_vint64()) as usize;
                println!("kv_count: {:?}", kv_count);
                let key_length_buffer_size = to_opt!(self.reader.decode_vint64());
                println!("key_length_buffer_size: {:?}", key_length_buffer_size);
                let mut key_length_buffer = vec![0u8; key_length_buffer_size as usize];
                to_opt!(self.reader.read(&mut key_length_buffer));


                let decompressed_keys_lengths = compress::decompressor(&self.header
                                                                            .compression_codec
                                                                            .unwrap(),
                                                                       key_length_buffer.as_ref());

                let mut c = io::Cursor::new(decompressed_keys_lengths.unwrap());
                // let mut b = Vec::new();
                let mut lens: Vec<usize> = Vec::new();
                for _ in 0..kv_count {
                    let len = c.decode_vint64().unwrap() as usize;
                    println!("len: {:?}", len);
                    lens.push(len);
                }


                let key_length_buffer_size = to_opt!(self.reader.decode_vint64());
                println!("key_length_buffer_size: {:?}", key_length_buffer_size);
                let mut key_length_buffer = vec![0u8; key_length_buffer_size as usize];
                to_opt!(self.reader.read(&mut key_length_buffer));
                let decompressed_keys = compress::decompressor(&self.header
                                                                    .compression_codec
                                                                    .unwrap(),
                                                               key_length_buffer.as_ref());

                let mut c = io::Cursor::new(decompressed_keys.unwrap());
                let mut keys: Vec<ByteString> = Vec::new();
                for i in 0..kv_count {
                    // println!("key: {:?}", c.read_i64::<BigEndian>());
                    let mut k = vec![0; lens[i]];
                    to_opt!(c.read(&mut k)); // todo errors
                    keys.push(k)
                }


                let val_length_buffer_size = to_opt!(self.reader.decode_vint64());
                println!("val_length_buffer_size: {:?}", val_length_buffer_size);
                let mut val_length_buffer = vec![0u8; val_length_buffer_size as usize];
                to_opt!(self.reader.read(&mut val_length_buffer));


                let decompressed_val_lengths = compress::decompressor(&self.header
                                                                           .compression_codec
                                                                           .unwrap(),
                                                                      val_length_buffer.as_ref());

                let mut c = io::Cursor::new(decompressed_val_lengths.unwrap());
                // let mut b = Vec::new();
                let mut lens: Vec<usize> = Vec::new();
                for _ in 0..kv_count {
                    let len = c.decode_vint64().unwrap() as usize;
                    println!("val_len: {:?}", len);
                    lens.push(len);
                }

                println!("{:?}", lens);


                let val_length_buffer_size = to_opt!(self.reader.decode_vint64());
                println!("val_length_buffer_size: {:?}", val_length_buffer_size);
                let mut val_length_buffer = vec![0u8; val_length_buffer_size as usize];
                to_opt!(self.reader.read(&mut val_length_buffer));
                let decompressed_vals = compress::decompressor(&self.header
                                                                    .compression_codec
                                                                    .unwrap(),
                                                               val_length_buffer.as_ref());

                let mut c = io::Cursor::new(decompressed_vals.unwrap());
                for i in 0..kv_count {
                    let mut v = vec![0; lens[i]]; //todo: reuse
                    to_opt!(c.read(&mut v));

                    self.block_buffer.push((keys[i].clone(), v));
                }
            }

            let len = self.block_buffer.len();
            if len > 0 {
                println!("Some");
                return Some(self.block_buffer.remove(0));
            } else {
                println!("None");
                return None;
            }
        } else {
            None
        }
    }
}


fn read_kv<R: io::Read>(kv_length: isize,
                        header: &Header,
                        reader: &mut R)
                        -> Result<(Vec<u8>, Vec<u8>)> {

    if header.compression_type == CompressionType::Block {
        let blocksize = try!(reader.decode_vint64());
        println!("{:?}", blocksize);

        let key = Vec::new();
        let value = Vec::new();
        Ok((key, value))
    } else {
        let k_length = try!(reader.read_i32::<BigEndian>()) as usize;

        let k_start = 0;
        let k_end = k_start + (k_length - 0);
        let v_start = k_end;
        let v_end = v_start + (kv_length as usize - k_length);

        // TODO: DataInput/DataOutput semantics per type
        // e.g. impl LongWritable...
        // re-implement common writables
        // core interface: Iterator<(u[8], u[8])> or a KV struct
        // mixin interface on KVstruct could extract key/value from
        // bytes
        println!("k_start: {:?}, k_end: {:?}, v_start: {:?}, v_end: {:?}",
                 k_start,
                 k_end,
                 v_start,
                 v_end);

        println!("{:?}", header);

        let mut buffer = vec![0; kv_length as usize];
        try!(reader.read(&mut buffer));

        let key = buffer[k_start..k_end].to_vec();

        if header.compression_type == CompressionType::Record {
            if let Some(ref codec) = header.compression_codec {
                let decompressed = try!(compress::decompressor(codec, &buffer[v_start..v_end]));

                Ok((key, decompressed))
            } else {
                panic!("WAT")
            }
        } else {
            let value = buffer[v_start..v_end].to_vec();

            Ok((key, value))
        }
    }
}

fn read_string<R: io::Read>(reader: &mut R) -> Result<String> {
    // read one byte, value len
    let value_length = try!(reader.read_u8()) as usize;
    let mut string = vec![0; value_length];

    try!(reader.read(&mut string));
    str::from_utf8(&string).map(|v| v.to_owned()).map_err(|e| Error::BadEncoding(e))
}

#[cfg(test)]
mod tests {
    use reader;
    use errors::Result;
    use std::path::Path;
    use std::fs::File;

    use byteorder::{BigEndian, ByteOrder};


    #[test]
    fn reads_standard_sequencefile() {
        let kvs = main_read("test_data/abc_long_text_none.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    fn reads_deflate_record() {
        let kvs = main_read("test_data/abc_long_text_deflate_record.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    fn reads_gzip_record() {
        let kvs = main_read("test_data/abc_long_text_gzip_record.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    fn reads_bzip2_record() {
        let kvs = main_read("test_data/abc_long_text_bzip2_record.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    #[should_panic(expected = "unsupported codec")]
    fn reads_snappy_record() {
        match main_read("test_data/abc_long_text_snappy_record.seq") {
            Ok(val) => val,
            Err(err) => panic!("Failed to open sequence file: {}", err),
        };
    }

    #[test]
    fn reads_deflate_block() {
        let kvs = main_read("test_data/abc_long_text_deflate_block.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    fn reads_gzip_block() {
        let kvs = main_read("test_data/abc_long_text_gzip_block.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    fn reads_bzip2_block() {
        let kvs = main_read("test_data/abc_long_text_bzip2_block.seq").unwrap();

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    #[test]
    #[should_panic(expected = "unsupported codec")]
    fn reads_snappy_block() {
        match main_read("test_data/abc_long_text_snappy_block.seq") {
            Ok(val) => val,
            Err(err) => panic!("Failed to open sequence file: {}", err),
        };
    }

    #[test]
    #[should_panic(expected = "bad or missing magic")]
    fn read_checks_magic() {
        match main_read("test_data/bad_magic.seq") {
            Ok(val) => val,
            Err(err) => panic!("Failed to open sequence file: {}", err),
        };
    }

    fn main_read(filename: &str) -> Result<Vec<(i64, String)>> {
        let path = Path::new(filename);

        let file = try!(File::open(&path));
        let seqfile = try!(reader::Reader::new(file));

        let kvs = seqfile.map(|(key, value)| {
            (BigEndian::read_i64(&key),
             String::from_utf8_lossy(&value[2..value.len()]).to_string())
        });

        Ok(kvs.collect())
    }
}
