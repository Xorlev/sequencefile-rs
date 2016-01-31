# rust-sequencefile
Hadoop SequenceFile library for Rust

## Usage
```rust
let path = Path::new("/path/to/seqfile");
let file = File::open(&path).unwrap();

let seqfile = sequencefile::Reader::new(file);

for kv in seqfile {
    println!("{:?}", kv);
}
```

## Status
Prototype status! Currently supports reading out your garden-variety sequence file. Handles uncompressed sequencefiles
as well as deflate/value compressed files.

There's a lot more to do:
- [ ] Varint decoding
- [ ] Block compression
- [ ] Gzip support
- [ ] Snappy support
- [ ] 'Writables', e.g. generic deserialization for common Hadoop writable types
- [ ] Zero-copy implementation.
- [ ] Sequencefile metadata
- [ ] Better error handling
- [ ] Tests
- [ ] Writer
