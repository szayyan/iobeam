use std::{
    fs::File,
    os::{
        fd::{IntoRawFd, RawFd},
        unix::fs::MetadataExt,
    },
};

#[derive(Clone, Debug, Copy)]
pub struct FileResult {
    pub fd: RawFd,
    pub size: usize,
}

pub struct FileSystemHandler;

impl FileSystemHandler {
    // two blocking syscalls - todo: benchmark uring equivalent
    pub fn open_raw_fd(&self, path: &str) -> Result<FileResult, std::io::Error> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.size() as usize;
        let ffd = file.into_raw_fd();
        Ok(FileResult {
            fd: ffd,
            size: file_size,
        })
    }
}
