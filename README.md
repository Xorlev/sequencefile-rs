[![Build Status](https://travis-ci.org/Xorlev/rust-sequencefile.svg?branch=master)](https://travis-ci.org/Xorlev/rust-sequencefile)
[![crates.io](http://meritbadge.herokuapp.com/sequencefile)](https://crates.io/crates/sequencefile)

# rust-sequencefile
Hadoop SequenceFile library for Rust

[Documentation](https://xorlev.github.io/rust-sequencefile/)

```toml
# Cargo.toml
[dependencies]
sequencefile = "0.1.2"
```

## Status
Prototype status! I'm in the process of learning Rust. :) Feedback appreciated.

Unfortunately that means the API will change. If you depend on this crate, please fully qualify your versions
for now.

Currently supports reading out your garden-variety sequence file. Handles uncompressed sequencefiles
as well as record compressed files (deflate only). The most common type of sequence file, block compressed,
isn't supported yet.

There's a lot more to do:
- [ ] Varint decoding
 - Block sizes are written with Varints
- [ ] Block decompression
- [X] Gzip support
- [X] Bzip2 support
- [ ] Snappy support
- [ ] 'Writables', e.g. generic deserialization for common Hadoop writable types
 - TODO: "Reflection" of some sort to allow registration of custom types.
- [ ] Zero-copy implementation.
- [ ] Sequencefile metadata
- [ ] Better documentation
- [X] Better error handling
- [X] Tests
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

## License
rust-sequencefile is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0),
with portions covered by various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
