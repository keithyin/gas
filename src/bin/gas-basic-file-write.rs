// Write some text to the file

use std::{io::Write, time::Instant};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    pub out_path: String,
    #[arg(long="mode", default_value = "vanilla")]
    pub mode: String,
}

fn vanilla_file_write(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 10; // 2 GB
    let data = vec![0_u8; data_size];
    // Open a file in write mode, creating it if it doesn't exist
    let mut file = std::fs::File::create(&cli.out_path).expect("Unable to create file");

    let mut start = 0;
    let buf_size = 4*  1024 * 1024; // 1 MB buffer size
    let instant = Instant::now();
    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + buf_size, data.len());
        file.write_all(&data[start..end])
            .expect("Unable to write data");
        start = end;
    }
    let elapsed = instant.elapsed().as_secs() as usize;
    let bytes_per_sec = (data_size / elapsed) as f64;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec); // 568MB/s . 4M block 640MB/s
}

fn main() {
    let cli = Cli::parse();
    match cli.mode.as_str() {
        "vanilla" => vanilla_file_write(&cli),
        _ => panic!("Unknown mode: {}", cli.mode),
        
    }
}
