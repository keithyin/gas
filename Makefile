build:
	cargo build --release

install:
	cp target/release/bam-gas-cvt /usr/bin
	cp target/release/gas-basic-file-write /usr/bin