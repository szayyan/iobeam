use std::{
    fs::File,
    os::{
        fd::{IntoRawFd, RawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path},
};

use anyhow::{Context, bail};
use arrayvec::ArrayString;
use percent_encoding::{percent_decode, percent_decode_str};
// use percent_encoding::{percent_decode, percent_decode_str};

#[derive(Clone, Debug, Copy)]
pub struct FileResult {
    pub fd: RawFd,
    pub size: usize,
}

pub struct FileSystemHandler {
    base_path: &'static str,
}

const LINUX_PATH_MAX_LENGTH: usize = 4096;
type PathBuffer = ArrayString<LINUX_PATH_MAX_LENGTH>;

impl FileSystemHandler {
    pub fn new(serve_dir: &'static str) -> anyhow::Result<Self> {
        if !Path::new(serve_dir).is_dir() {
            bail!("serve directory must be a directory")
        }

        Ok(FileSystemHandler {
            base_path: serve_dir,
        })
    }

    // warning: path must be constructed from valid utf8 or bad things will happen
    pub fn construct_and_validate_requested_path<'a>(
        &self,
        // buf: &mut PathBuffer,
        requested_path: &str,
    ) -> anyhow::Result<PathBuffer> {
        let mut buf =
            PathBuffer::from(self.base_path).context("Path buffer length exceeded by base path")?;

        let path = requested_path.trim_end_matches('/');
        let path_decoded = percent_decode_str(path).decode_utf8()?;
        let path_decoded = Path::new(&*path_decoded);

        for component in path_decoded.components() {
            match component {
                Component::Normal(c) => {
                    // safe - we constructed from valid utf8
                    let cstr = unsafe { std::str::from_utf8_unchecked(c.as_bytes()) };
                    if buf.try_push_str(cstr).is_err() {
                        bail!(
                            "Path buffer length exceeded. Total path length must be < PATH_MAX_LENGTH."
                        );
                    }
                }
                Component::CurDir => {}
                Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                    bail!("Request path must not contain traversal components or root dir.");
                }
            }
        }
        Ok(buf)
    }

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
