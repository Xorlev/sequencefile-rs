//! Implementation and structs for a sequencefile reader

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use std::io;
use std::io::BufReader;
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
    /// Sequencefile header
    pub header: Header,
    reader: BufReader<R>,
    block_buffer: Vec<(ByteString, ByteString)>,
    is_error: bool,
}

impl<R: io::Read> Reader<R> {
    /// Create a new Reader from an io::Read
    ///
    /// # Failures
    /// Returns an `Error` if sequencefile header is malformed, e.g. unsupported version or
    /// invalid compression algorithm
    pub fn new(r: R) -> Result<Reader<R>> {
        let mut br = BufReader::new(r);

        // TODO: handle this
        let header = try!(read_header(&mut br));

        Ok(Reader {
            header: header,
            reader: br,
            block_buffer: Vec::new(),
            is_error: false,
        })
    }
}

fn read_header<R: io::Read>(reader: &mut R) -> Result<Header> {
    let mut magic = [0; 3];
    try!(reader.read_exact(&mut magic));
    if magic != MAGIC.as_bytes() {
        return Err(Error::BadMagic(String::from_utf8_lossy(&magic).to_string()));
    }

    let mut version = [0; 1];
    try!(reader.read_exact(&mut version));
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
    try!(reader.read_exact(&mut flags));

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

    let pair_count = try!(reader.read_u32::<BigEndian>());
    let mut pairs: HashMap<String, String> = HashMap::new();
    for _ in 0..pair_count {
        let key = try!(read_buf(reader).map(|b| String::from_utf8_lossy(b.as_ref()).to_string()));
        let val = try!(read_buf(reader).map(|b| String::from_utf8_lossy(b.as_ref()).to_string()));

        pairs.insert(key, val);
    }

    let mut sync_marker = [0; SYNC_SIZE];
    try!(reader.read(&mut sync_marker));

    Ok(Header {
        version: version,
        compression_type: compression_type,
        compression_codec: compression_codec,
        key_class: key_class,
        value_class: value_class,
        metadata: pairs,
        sync_marker: sync_marker.to_vec(),
    })
}


impl<R: io::Read> Iterator for Reader<R> {
    type Item = Result<(ByteString, ByteString)>;

    fn next(&mut self) -> Option<Result<(ByteString, ByteString)>> {
        if self.is_error {
            return None;
        }

        match next_element(self) {
            Ok(val) => Some(Ok(val)),
            Err(Error::UnexpectedDecoderError(_)) | Err(Error::EOF) => {
                self.is_error = true;
                None
            }
            Err(val) => {
                self.is_error = true;
                Some(Err(val))
            }
        }
    }
}

fn next_element<R: io::Read>(reader: &mut Reader<R>) -> Result<(ByteString, ByteString)> {
    if reader.block_buffer.len() == 0 || reader.header.compression_type != CompressionType::Block {
        let mut last_sync_marker = [0; SYNC_SIZE];
        let kv_length = try!(reader.reader.read_i32::<BigEndian>()) as i64;

        // handle sync marker
        if kv_length == -1 {
            try!(reader.reader.read_exact(&mut last_sync_marker));
            if last_sync_marker.to_vec() != reader.header.sync_marker {
                return Err(Error::SyncMarkerMismatch);
            }
        }

        if reader.header.compression_type != CompressionType::Block {
            let (key, value) = try!(read_kv(kv_length as isize,
                                            &reader.header,
                                            &mut reader.reader));

            return Ok((key, value));
        }
    }

    if reader.header.compression_type == CompressionType::Block {
        let codec = &reader.header.compression_codec.unwrap();
        if reader.block_buffer.len() == 0 {
            // count of kvs in block
            let kv_count = try!(reader.reader.decode_vint64()) as usize;

            // key lengths
            let kl_buffer = try!(read_buf(&mut reader.reader));
            let key_lengths = compress::decompressor(codec, kl_buffer.as_ref());
            let mut c = try!(key_lengths.map(|kl| io::Cursor::new(kl)));
            let mut lens: Vec<usize> = Vec::with_capacity(kv_count);
            for _ in 0..kv_count {
                let len = try!(c.decode_vint64()) as usize;
                lens.push(len);
            }

            let key_length_buffer = try!(read_buf(&mut reader.reader));
            let decompressed_keys = compress::decompressor(codec, key_length_buffer.as_ref());
            let mut c = try!(decompressed_keys.map(|kl| io::Cursor::new(kl)));
            let mut keys: Vec<ByteString> = Vec::with_capacity(kv_count);
            for i in 0..kv_count {
                let mut k = vec![0; lens[i]];
                try!(c.read_exact(&mut k)); // todo errors
                keys.push(k)
            }

            let val_lengths = try!(read_buf(&mut reader.reader));
            let decompressed_val_lengths = compress::decompressor(codec, val_lengths.as_ref());

            let mut c = try!(decompressed_val_lengths.map(|kl| io::Cursor::new(kl)));
            let mut lens: Vec<usize> = Vec::with_capacity(kv_count);
            for _ in 0..kv_count {
                let len = c.decode_vint64().unwrap() as usize;
                lens.push(len);
            }

            let val_buffer = try!(read_buf(&mut reader.reader));
            let decompressed_vals = compress::decompressor(codec, val_buffer.as_ref());

            let mut c = try!(decompressed_vals.map(|kl| io::Cursor::new(kl)));
            for i in 0..kv_count {
                let mut v = vec![0; lens[i]]; //todo: reuse
                try!(c.read_exact(&mut v));

                reader.block_buffer.push((keys.remove(0), v));
            }
        }

        let len = reader.block_buffer.len();
        if len > 0 {
            Ok(reader.block_buffer.remove(0))
        } else {
            Err(Error::EOF)
        }
    } else {
        Err(Error::EOF)
    }
}

fn read_kv<R: io::Read>(kv_length: isize,
                        header: &Header,
                        reader: &mut R)
                        -> Result<(Vec<u8>, Vec<u8>)> {
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
    // println!("k_start: {:?}, k_end: {:?}, v_start: {:?}, v_end: {:?}",
    //  k_start,
    //  k_end,
    //  v_start,
    //  v_end);

    // println!("{:?}", header);

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

fn read_string<R: io::Read>(reader: &mut R) -> Result<String> {
    // read one byte, value len
    let value_length = try!(reader.read_u8()) as usize;
    let mut string = vec![0; value_length];

    try!(reader.read_exact(&mut string));
    str::from_utf8(&string).map(|v| v.to_owned()).map_err(|e| Error::BadEncoding(e))
}

fn read_buf<R: io::Read>(reader: &mut R) -> Result<Vec<u8>> {
    let key_len = try!(reader.decode_vint64()) as usize;
    let mut key_buf = vec![0; key_len];
    try!(reader.read_exact(&mut key_buf));

    Ok(key_buf)
}

#[cfg(test)]
mod tests {
    use reader;
    use errors::Result;
    use std::path::Path;
    use std::fs::File;
    use byteorder::{BigEndian, ByteOrder};

    macro_rules! test_std {
        ($e:ident) => {
            #[test]
            fn $e() {
                let filename = format!("test_data/{}.seq", stringify!($e));
                let kvs = match main_read(filename.as_ref()) {
                    Ok(val) => val,
                    Err(e) => panic!("{}", e),
                };

                assert_eq!(26, kvs.len());
                assert_eq!((0, "A".to_string()), kvs[0]);
                assert_eq!((25, "Z".to_string()), kvs[25]);
            }
        }
    }

    test_std!(abc_long_text_none);

    test_std!(abc_long_text_deflate_block);
    test_std!(abc_long_text_deflate_record);

    test_std!(abc_long_text_gzip_record);
    test_std!(abc_long_text_gzip_block);

    test_std!(abc_long_text_bzip2_record);
    test_std!(abc_long_text_bzip2_block);

    // TODO: Snappy support
    // test_std!(abc_long_text_snappy_record);
    // test_std!(abc_long_text_snappy_block);


    #[test]
    fn reads_metadata() {
        let sf = reader_for("test_data/metadata.seq").unwrap();

        println!("{:?}", sf.header.metadata);

        assert_eq!("b", sf.header.metadata.get("a").unwrap());
        assert_eq!("z", sf.header.metadata.get("y").unwrap());
    }

    #[test]
    #[should_panic(expected = "bad or missing magic")]
    fn read_checks_magic() {
        match main_read("test_data/bad_magic.seq") {
            Ok(val) => val,
            Err(err) => panic!("Failed to open sequence file: {}", err),
        };
    }

    fn reader_for(filename: &str) -> Result<reader::Reader<File>> {
        let path = Path::new(filename);
        let file = try!(File::open(&path));

        Ok(try!(reader::Reader::new(file)))
    }

    fn main_read(filename: &str) -> Result<Vec<(i64, String)>> {
        let seqfile = try!(reader_for(filename));

        let kvs = seqfile.map(|e| e.unwrap()).map(|(key, value)| {
            (BigEndian::read_i64(&key),
             String::from_utf8_lossy(&value[2..value.len()]).to_string())
        });

        Ok(kvs.collect())
    }
}
