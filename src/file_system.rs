use std::{
    fs::File,
    os::{
        fd::{IntoRawFd, RawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path},
};

use anyhow::bail;
use arrayvec::ArrayString;
// use percent_encoding::{percent_decode, percent_decode_str};

#[derive(Clone, Debug, Copy)]
pub struct FileResult {
    pub fd: RawFd,
    pub size: usize,
}

pub struct FileSystemHandler {
    base_path: PathBuffer,
}

const LINUX_PATH_MAX_LENGTH: usize = 4096;
type PathBuffer = ArrayString<LINUX_PATH_MAX_LENGTH>;

impl FileSystemHandler {
    pub fn new(serve_dir: &str) -> anyhow::Result<Self> {
        if !Path::new(serve_dir).is_dir() {
            bail!("serve directory must be a directory")
        }

        let Ok(base_path) = PathBuffer::from(serve_dir) else {
            bail!("serve dir length > MAX PATH LENGTH")
        };

        Ok(FileSystemHandler { base_path })
    }

    // warning path must be constructed from valid utf8 or undefined behaviour
    pub fn construct_and_validate_decoded_path<'a>(
        &self,
        path: &Path,
    ) -> anyhow::Result<PathBuffer> {
        let mut buf = self.base_path; // bwise stack copy - v. cheap
        for component in path.components() {
            match component {
                Component::Normal(c) => {
                    let cstr = unsafe { std::str::from_utf8_unchecked(c.as_bytes()) };
                    if buf.try_push('/').is_err() {
                        bail!(
                            "Request path buffer length exceeded. Total path length must be < PATH_MAX_LENGTH."
                        );
                    }
                    if buf.try_push_str(cstr).is_err() {
                        bail!(
                            "Request path buffer length exceeded. Total path length must be < PATH_MAX_LENGTH."
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

    pub fn open_raw_ffd(&self, path: &str) -> anyhow::Result<FileResult, std::io::Error> {
        let file = File::open(path)?;
        let mdata = file.metadata()?;

        if mdata.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                "Requested path must be a file.",
            ));
        }

        let ffd = file.into_raw_fd();
        Ok(FileResult {
            fd: ffd,
            size: mdata.size() as _,
        })
    }
}
