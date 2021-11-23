[![Build Status](https://travis-ci.org/Xorlev/rust-sequencefile.svg?branch=master)](https://travis-ci.org/Xorlev/rust-sequencefile)
[![crates.io](http://meritbadge.herokuapp.com/sequencefile)](https://crates.io/crates/sequencefile)
[![Coverage Status](https://coveralls.io/repos/github/Xorlev/rust-sequencefile/badge.svg?branch=master)](https://coveralls.io/github/Xorlev/rust-sequencefile?branch=master)

# rust-sequencefile
Hadoop SequenceFile library for Rust

[Documentation](https://xorlev.github.io/rust-sequencefile/)

```toml
# Cargo.toml
[dependencies]
sequencefile = "0.2.0"
```

## Status
Prototype status! I'm in the process of learning Rust. :) Feedback appreciated.

Unfortunately that means the API will change. If you depend on this crate, please fully qualify your versions
for now.

Currently supports reading out your garden-variety sequence file. Handles uncompressed sequencefiles
as well as block/record compressed files (deflate, gzip, and bzip2 only). LZO and Snappy are not (yet) handled.

There's a lot more to do:
- [X] Varint decoding
 - Block sizes are written with Varints
- [X] Block decompression
- [X] Gzip support
- [X] Bzip2 support
- [X] Sequencefile metadata
- [X] Better error handling
- [X] Tests
- [X] Better error handling2
- [ ] More tests
- [ ] Better documentation
- [ ] Snappy support
- [ ] CRC file support
- [X] 'Writables', e.g. generic deserialization for common Hadoop writable types
- [ ] Writer
- [ ] Gracefully handle version 4 sequencefiles
- [ ] Zero-copy implementation.
- [ ] LZO support.

### Benchmarks

There aren't any formal benchmarks yet. However with deflate on my early 2012 MBP, 98.4% of CPU time
was spent in miniz producing ~125MB/s of decompressed data.

## Usage
```rust
let path = Path::new("/path/to/seqfile");
let file = File::open(&path).unwrap();

let seqfile = sequencefile::Reader::new(file).expect("Failed to open sequence file.");

for kv in seqfile {
    println!("{:?}", kv); // Some(([123, 123], [456, 456]))
}

// Until there's automatic deserialization, you can do something like this:
// VERY hacky
let kvs = seqfile.map(|e| e.unwrap()).map(|(key, value)| {
    (BigEndian::read_i64(&key),
     String::from_utf8_lossy(&value[2..value.len()]).to_string())
});

for (k,v) in kvs {
  println!("key: {}, value: {}", k, v);
}
```

## License
rust-sequencefile is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0),
with portions covered by various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
