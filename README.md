[![Build Status](https://travis-ci.org/Xorlev/rust-sequencefile.svg?branch=master)](https://travis-ci.org/Xorlev/rust-sequencefile)

# rust-sequencefile
Hadoop SequenceFile library for Rust

## Status
Prototype status! I'm in the process of learning Rust. :) Feedback appreciated.

Currently supports reading out your garden-variety sequence file. Handles uncompressed sequencefiles
as well as deflate/value compressed files. The most common type of sequence file, block compressed,
isn't supported yet.

There's a lot more to do:
- [ ] Varint decoding
 - Block sizes are written with Varints
- [ ] Block decompression
- [ ] Gzip support
- [ ] Snappy support
- [ ] 'Writables', e.g. generic deserialization for common Hadoop writable types
 - TODO: "Reflection" of some sort to allow registration of custom types.
- [ ] Zero-copy implementation.
- [ ] Sequencefile metadata
- [ ] Better error handling
- [ ] Tests
- [ ] Writer

## Usage
```rust
let path = Path::new("/path/to/seqfile");
let file = File::open(&path).unwrap();

let seqfile = sequencefile::Reader::new(file);

for kv in seqfile {
    println!("{:?}", kv);
}
```
