use std::{
    fs,
    io::Write,
    num::NonZero,
    path::{self, Path},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use crossbeam::channel::{Receiver, Sender};

struct StartEnd(pub (usize, usize));

/// 存储序列化的对象，核心实现是二级存储
/// 开头的 u32 存储 文件格式的版本，之后的 u32 存储的是 一级索引的长度，一级索引是 Vec<(usize, usize)> 序列化的结果。
///     一级索引 提供 2M 空间进行存储。
/// 之后每1000次写入会记录其每次写入的位置。所以gasfile 最多大概支持 32,000,000 次写入。
/// ----file
/// u32,u32,Vec<(usize, usize)>(2M+8bytes) .....(1000 write) positionsOfEachWrite
pub struct GasFile<T> {
    version: u32,
    fname: path::PathBuf,
    threads: usize,
    current_position: AtomicUsize,
    worker_threads_started_flag: AtomicBool,

    wirter_sender: Mutex<Option<Sender<T>>>,
    reader_recv: Mutex<Option<Receiver<T>>>,
}

impl<T> GasFile<T>
where
    T: Send + 'static,
{
    pub fn new<P>(p: P, threads: NonZero<usize>) -> Arc<Self>
    where
        P: AsRef<Path>,
    {
        let p = p.as_ref().to_owned();
        Self {
            version: 1,
            fname: p.into(),
            threads: threads.get(),
            current_position: AtomicUsize::new(2 * 1024 * 1024 + 8), // 2M + 8bytes
            worker_threads_started_flag: AtomicBool::new(false),
            wirter_sender: Mutex::new(None),
            reader_recv: Mutex::new(None),
        }
        .into()
    }
    pub fn start_write_worker(self: &Arc<Self>) {
        let mut file = fs::File::create(&self.fname).unwrap();
        file.write_all(&self.version.to_le_bytes()).unwrap();

        let (sender, recv) = crossbeam::channel::bounded::<T>(1000);
        for idx in 0..self.threads {}
    }

    pub fn writer(self: Arc<Self>) {}
}
