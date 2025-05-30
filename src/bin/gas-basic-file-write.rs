// Write some text to the file

use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt, time::Instant};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    pub out_path: String,
    #[arg(long = "mode", default_value = "vanilla")]
    pub mode: String,
}

fn vanilla_file_write(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 10; // 2 GB
    let data = vec![0_u8; data_size];
    // Open a file in write mode, creating it if it doesn't exist
    let mut file = std::fs::File::create(&cli.out_path).expect("Unable to create file");

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
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

/// 分配 O_DIRECT 需要的对齐缓冲区
fn aligned_alloc(size: usize) -> Vec<u8> {
    use std::ptr;
    let align = 512;
    let mut ptr: *mut u8 = ptr::null_mut();
    unsafe {
        let ret = libc::posix_memalign(&mut ptr as *mut _ as *mut _, align, size);
        if ret != 0 {
            panic!("posix_memalign failed");
        }
        Vec::from_raw_parts(ptr, size, size)
    }
}

fn vanilla_file_write_dio(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 10; // 2 GB
    let data = aligned_alloc(data_size);
    // Open a file in write mode, creating it if it doesn't exist
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT) // Use O_DIRECT for direct I/O
        .open(&cli.out_path)
        .expect("Unable to create file");

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
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
        "vanilla-dio" => vanilla_file_write_dio(&cli),
        _ => panic!("Unknown mode: {}", cli.mode),
    }
}
