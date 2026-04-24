use std::{net::TcpListener, os::fd::AsRawFd};

use io_uring::{IoUring, types::Fd};
use slab::Slab;

use crate::{
    buffer_pool::BufferPool, cli::Cli, file_system::FileSystemHandler, uring_server::UringServer,
};

mod buffer_pool;
mod cli;
mod file_system;
mod http;
mod uring_server;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let file_system_handler = FileSystemHandler::new(&cli.dir)?;
    let token_alloc = Slab::with_capacity(64);
    let mut buffer_pool = BufferPool::new(64);
    buffer_pool.allocate_range(64);

    let mut ring = IoUring::new(cli.uring_entries)?;
    let listener = TcpListener::bind((cli.host.as_str(), cli.port))?;
    let listener_fd = Fd(listener.as_raw_fd());

    println!("listening {}", listener.local_addr()?);

    let mut server = UringServer::new(
        listener_fd,
        cli.write_strategy,
        file_system_handler,
        &mut ring,
        buffer_pool,
        token_alloc,
        cli.body_write_chunk_size,
    );

    server.start_event_loop()?;

    Ok(())
}
