[![Build Status](https://travis-ci.org/Xorlev/rust-sequencefile.svg?branch=master)](https://travis-ci.org/Xorlev/rust-sequencefile)

# rust-sequencefile
Hadoop SequenceFile library for Rust

## Status
Prototype status! I'm in the process of learning Rust. :) Feedback appreciated.

Currently supports reading out your garden-variety sequence file. Handles uncompressed sequencefiles
as well as record compressed files (deflate only). The most common type of sequence file, block compressed,
isn't supported yet.

There's a lot more to do:
- [ ] Varint decoding
 - Block sizes are written with Varints
- [ ] Block decompression
- [X] Gzip support
- [ ] Snappy support
- [ ] 'Writables', e.g. generic deserialization for common Hadoop writable types
 - TODO: "Reflection" of some sort to allow registration of custom types.
- [ ] Zero-copy implementation.
- [ ] Sequencefile metadata
- [X] Better error handling
- [ ] Tests
- [ ] Writer

## Usage
```rust
let path = Path::new("/path/to/seqfile");
let file = File::open(&path).unwrap();

let seqfile = match sequencefile::Reader::new(file) {
  Ok(val) => val,
  Err(err) => panic!("Failed to open sequence file: {}", err),
}

for kv in seqfile {
    println!("{:?}", kv);
}
```
