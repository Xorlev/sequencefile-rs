use byteorder::{BigEndian, ByteOrder};
use errors::Result;
use reader;
use std::fs::File;
use std::path::Path;

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
    };
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

// #[test]
// fn read_webgraph_nodes() {
//     let reader = reader_for("test_data/nodes.seq").expect("cannot open sequence file");
//     println!("{}", reader.header.key_class);
//     println!("{}", reader.header.value_class);
//     let mut output = File::create("keys.txt").expect("cannot open output file");
//     for (key, _) in reader.flatten() {
//         writeln!(output, "{:?}", OsStr::from_bytes(&key)).unwrap();
//     }
// }

fn reader_for(filename: &str) -> Result<reader::Reader<File>> {
    let path = Path::new(filename);
    let file = File::open(&path)?;

    reader::Reader::new(file)
}

fn main_read(filename: &str) -> Result<Vec<(i64, String)>> {
    let seqfile = reader_for(filename)?;

    let kvs = seqfile.map(|e| e.unwrap()).map(|(key, value)| {
        (
            BigEndian::read_i64(&key),
            String::from_utf8_lossy(&value[2..value.len()]).to_string(),
        )
    });

    Ok(kvs.collect())
}
