lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --release -- -D warnings

tests:
	cargo test --tests --release -- --test-threads=1

release:
	cargo build --all-targets --release

clean:
	cargo clean

all: clean lint tests release