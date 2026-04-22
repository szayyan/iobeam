use std::{net::TcpListener, os::fd::AsRawFd};

use io_uring::{IoUring, types::Fd};
use slab::Slab;

use crate::{
    buffer_pool::BufferPool,
    file_system::FileSystemHandler,
    uring_server::{UringServer, WriteStrategy},
};

mod buffer_pool;
mod file_system;
mod http;
mod uring_server;

fn main() -> anyhow::Result<()> {
    let file_system_handler = FileSystemHandler::new(".")?;
    let token_alloc = Slab::with_capacity(64);
    let buffer_pool = BufferPool::new(64);
    let mut ring = IoUring::new(256)?;
    let listener = TcpListener::bind(("127.0.0.1", 3456))?;
    let listener_fd = Fd(listener.as_raw_fd());

    println!("listening {}", listener.local_addr()?);

    let mut server = UringServer::new(
        listener_fd,
        WriteStrategy::PipeAndSplice,
        file_system_handler,
        &mut ring,
        buffer_pool,
        token_alloc,
    );

    server.start_event_loop()?;

    Ok(())
}
