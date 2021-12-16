[![CircleCI](https://circleci.com/gh/Xorlev/sequencefile-rs/tree/master.svg?style=svg)](https://circleci.com/gh/Xorlev/sequencefile-rs/tree/master)
[![Crates.io](https://img.shields.io/crates/v/sequencefile)](https://crates.io/crates/sequencefile)

# sequencefile-rs
Hadoop SequenceFile library for Rust

[Documentation](https://docs.rs/sequencefile)

```toml
# Cargo.toml
[dependencies]
sequencefile = "0.2.0"
```

## Status
Prototype status!

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

There are only two benchmarks yet. Those two benchmarks read seq files (1000 entries each) generated in java with no compression. Both have Text as keyclass. First has i64 as valueclass, second has some more complex structure. 
Earlier investigations (with deflate on an early 2012 MBP) showed 98.4% of CPU time was spent in miniz producing ~125MB/s of decompressed data.

## Usage
```rust
use sequencefile::Writable;
let file = File::open("/path/to/seqfile").expect("cannot open file");

struct ValueClass {
  // some fields
}

impl Writable for ValueClass {
   fn read(buf: &mut impl std::io::Read) -> sequencefile::Result<Self>
    where
        Self: Sized,
    {
      // implement read function
    }
}

let seqfile = sequencefile::Reader::<File, Text, ValueClass>::new(file).expect("cannot open reader");

for kv in seqfile.flatten() {

    println!("{:?} - {:?}", kv.0, kv.1);
}
```

## License
sequencefile-rs is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0),
with portions covered by various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
