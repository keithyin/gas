use std::{
    fs,
    io::{Seek, Write},
    num::NonZero,
    ops::{Deref, DerefMut},
    path::{self, Path},
    sync::{Arc, Mutex, atomic::AtomicBool},
    thread,
};

use bincode::config::Configuration;
use crossbeam::channel::{Receiver, Sender};

pub fn get_bincode_cfg() -> Configuration {
    bincode::config::standard()
        .with_little_endian()
        .with_variable_int_encoding()
}

#[derive(Debug, Clone, Default, bincode::Encode, bincode::Decode)]
struct WritePositions(pub Vec<u64>);
impl Deref for WritePositions {
    type Target = Vec<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for WritePositions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, Default, bincode::Encode)]
struct WritePositionsMeta(Vec<(u64, u64)>);
impl Deref for WritePositionsMeta {
    type Target = Vec<(u64, u64)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WritePositionsMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct Locations {
    pub cur_position: u64,
    pub write_positions: WritePositions,
    pub write_positions_meta: WritePositionsMeta,
}

#[allow(unused)]
impl Locations {
    pub fn new(cur_pos: u64) -> Self {
        Self {
            cur_position: cur_pos,
            write_positions: WritePositions::default(),
            write_positions_meta: WritePositionsMeta::default(),
        }
    }
}

impl Default for Locations {
    fn default() -> Self {
        Self {
            cur_position: 2 * 1024 * 1024 + 8, // 初始位置为2M + 8bytes
            write_positions: WritePositions(vec![]),
            write_positions_meta: WritePositionsMeta(vec![]),
        }
    }
}

/// 存储序列化的对象，核心实现是二级存储
/// 开头的 u32 存储 文件格式的版本，之后的 u32 存储的是 一级索引的长度，一级索引是 Vec<(usize, usize)> 序列化的结果。
///     一级索引 提供 2M 空间进行存储。
/// 之后每1000次写入会记录其每次写入的位置。所以gasfile 最多大概支持 32,000,000 次写入。
/// ----file
/// u32,u32,Vec<(usize, usize)>(2M+8bytes) .....(1000 write) positionsOfEachWrite
pub struct GasFileWriter {
    version: u32,
    fname: path::PathBuf,
    threads: usize,
    positions: Mutex<Locations>,
    worker_threads_started_flag: AtomicBool,
    writer_recv: Receiver<Vec<u8>>,

    handlers: Mutex<Option<Vec<thread::JoinHandle<()>>>>,
}

impl GasFileWriter {
    ///
    /// sender is used for to send data to be written. the data should be bytes stream
    pub fn new_writer<P>(p: P, threads: NonZero<usize>) -> (Arc<Self>, Sender<Vec<u8>>)
    where
        P: AsRef<Path>,
    {
        let (sender, recv) = crossbeam::channel::bounded::<Vec<u8>>(1000);

        let p = p.as_ref().to_owned();
        (
            Self {
                version: 1,
                fname: p.into(),
                threads: threads.get(),
                positions: Mutex::new(Locations::default()),
                worker_threads_started_flag: AtomicBool::new(false),
                writer_recv: recv,
                handlers: Mutex::new(Some(vec![])),
            }
            .into(),
            sender,
        )
    }
    pub fn start_write_worker(self: &Arc<Self>) {
        if self
            .worker_threads_started_flag
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.worker_threads_started_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut file = fs::File::create(&self.fname).unwrap();
        file.write_all(&self.version.to_le_bytes()).unwrap();

        for _ in 0..self.threads {
            let handler = {
                let self_clone = Arc::clone(self);
                thread::spawn(move || {
                    self_clone.write_worker();
                })
            };
            self.handlers
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(handler);
        }
    }

    fn write_worker(self: &Arc<Self>) {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.fname)
            .unwrap();
        let recv = self.writer_recv.clone();
        for data in recv {
            self.write(&data, &mut file);
        }
    }

    fn write(self: &Arc<Self>, data: &[u8], file: &mut fs::File) {
        let cur_pos = {
            let mut locations = self.positions.lock().unwrap();
            let cur_pos = locations.cur_position;
            locations.cur_position += data.len() as u64;
            locations.write_positions.push(cur_pos);
            cur_pos
        };

        file.seek(std::io::SeekFrom::Start(cur_pos as u64)).unwrap();
        file.write_all(data).unwrap();

        let value2write = {
            let mut locations = self.positions.lock().unwrap();

            // 每1000次写入记录一次位置

            let value2write = if locations.write_positions.len() >= 1000 {
                let cfg = get_bincode_cfg();
                let serialize = bincode::encode_to_vec(&locations.write_positions, cfg).unwrap();
                let write_pos = locations.cur_position;
                locations.cur_position += serialize.len() as u64;

                locations
                    .write_positions_meta
                    .push((cur_pos, serialize.len() as u64));
                locations.write_positions.clear();
                Some((write_pos, serialize))
            } else {
                None
            };
            locations
                .write_positions_meta
                .push((cur_pos, data.len() as u64));
            value2write
        };

        if let Some((write_pos, serialize)) = value2write {
            file.seek(std::io::SeekFrom::Start(write_pos as u64))
                .unwrap();
            file.write_all(&serialize).unwrap();
        }
    }
}

impl Drop for GasFileWriter {
    fn drop(&mut self) {
        // 等待所有线程结束
        let mut handlers = self.handlers.lock().unwrap();
        for handler in handlers.take().unwrap() {
            handler.join().unwrap();
        }

        // 写入一级索引
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&self.fname)
            .unwrap();

        let cfg = get_bincode_cfg();
        let serialize =
            bincode::encode_to_vec(&self.positions.lock().unwrap().write_positions_meta, cfg)
                .unwrap();

        file.seek(std::io::SeekFrom::Start(4)).unwrap();
        file.write_all(&(serialize.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&serialize).unwrap();
    }
}
