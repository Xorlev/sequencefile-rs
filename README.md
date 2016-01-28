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
Prototype status! There's a lot more to do:
- [ ] Block compression
- [ ] 'Writables', e.g. generic deserialization for common Hadoop writable types
- [ ] Writer
- [ ] Zero-copy implementation.
- [ ] Sequencefile metadata
- [ ] Better error handling
- [ ] Tests
