extern crate sequencefile;
use std::{
    collections::HashMap,
    fs::File,
    io::{Cursor, Read},
};

use sequencefile::{writable::Writable, Reader, Text};

#[allow(dead_code)]
struct Simple {
    s1: i64,
}

impl Writable for Simple {
    fn read(buf: &mut impl std::io::Read) -> sequencefile::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            s1: i64::read(buf)?,
        })
    }
}

#[allow(dead_code)]
struct Complex {
    s1: i64,
    s2: u8,
    s3: i16,
    s4: HashMap<String, String>,
}

impl Writable for Complex {
    fn read(buf: &mut impl std::io::Read) -> sequencefile::Result<Self>
    where
        Self: Sized,
    {
        let s1 = i64::read(buf)?;
        let s2 = u8::read(buf)?;
        let s3 = i16::read(buf)?;
        let len = i32::read(buf)?;
        let mut s4 = HashMap::<String, String>::new();
        for _ in 0..len {
            let key = Text::read(buf)?;
            let val = Text::read(buf)?;
            s4.insert(key.to_string().to_string(), val.to_string().to_string());
        }
        Ok(Self { s1, s2, s3, s4 })
    }
}

fn read_from_file<K: Writable, V: Writable>(path: &str) {
    let file = File::open(path).expect("cannot open input file");
    let reader = Reader::<File, K, V>::new(file).expect("cannot open reader");
    for kvp in reader.flatten() {
        criterion::black_box(kvp);
    }
}

fn read_from_memory<K: Writable, V: Writable>(buf: &[u8]) {
    let reader = Reader::<Cursor<&[u8]>, K, V>::new(Cursor::new(buf)).expect("cannot open reader");
    for kvp in reader.flatten() {
        criterion::black_box(kvp);
    }
}

fn criterion_benchmark(c: &mut criterion::Criterion) {
    c.bench_function("read simple from file", |b| {
        b.iter(|| read_from_file::<Text, Simple>("./test_data/simple.seq"))
    });
    c.bench_function("read complex from file", |b| {
        b.iter(|| read_from_file::<Text, Complex>("./test_data/complex.seq"))
    });
    c.bench_function("read simple from memory", |b| {
        let mut file = File::open("./test_data/simple.seq").expect("cannot open input file");
        let mut buf: Vec<u8> = vec![];
        file.read_to_end(&mut buf)
            .expect("cannot read file content");
        b.iter(|| read_from_memory::<Text, Simple>(&buf))
    });
    c.bench_function("read complex from memory", |b| {
        let mut file = File::open("./test_data/complex.seq").expect("cannot open input file");
        let mut buf: Vec<u8> = vec![];
        file.read_to_end(&mut buf)
            .expect("cannot read file content");
        b.iter(|| read_from_memory::<Text, Complex>(&buf))
    });
}

criterion::criterion_group!(benches, criterion_benchmark);
criterion::criterion_main!(benches);
