use crate::{
    buffer_pool::BufferPool, cli::Cli, file_system::FileSystemHandler, uring_server::UringServer,
};
use anyhow::{Context, bail};
use io_uring::{IoUring, Probe, opcode, types::Fd};
use slab::Slab;
use std::net::Ipv4Addr;

mod buffer_pool;
mod cli;
mod file_system;
mod http;
#[macro_use]
mod log;
mod uring_server;

fn main() -> anyhow::Result<()> {
    check_feature_compatibility()?;

    let cli = Cli::parse();

    info!("starting server on {}:{}", &cli.bind, cli.port);

    let handles: Vec<_> = (0..cli.threads)
        .map(|_| {
            let cli = cli.clone();
            std::thread::spawn(move || -> anyhow::Result<()> {
                // this fd is not closed by the program
                // bc its unncessary. a benign fd leak
                let listener = make_tcp_listener(&cli.bind, cli.port, cli.backlog)?;
                let file_system_handler = FileSystemHandler::new(&cli.dir)?;
                let token_alloc = Slab::with_capacity(1024);
                let mut buffer_pool = BufferPool::with_capacity(1024);
                buffer_pool.allocate_range(1024);
                let mut ring = IoUring::new(cli.uring_entries)?;

                let mut server = UringServer::new(
                    listener,
                    cli.write_strategy,
                    file_system_handler,
                    &mut ring,
                    buffer_pool,
                    token_alloc,
                    cli.body_write_chunk_size,
                );
                server.start_event_loop()?;

                Ok(())
            })
        })
        .collect();

    for handle in handles {
        if let Err(e) = handle.join() {
            error!("Worker thread panicked: {:?}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

fn check_feature_compatibility() -> anyhow::Result<()> {
    let temp_ring = IoUring::new(1)?;
    let mut probe = Probe::new();
    temp_ring.submitter().register_probe(&mut probe)?;
    // most recent feature
    if !probe.is_supported(opcode::AcceptMulti::CODE) {
        bail!(
            "iobeam uses the AcceptMulti opcode\
            which was introduced in kernel 5.19\n\
            Please use a kernel >=5.19 \
            and ensure 'sysctl kernel.io_uring_disabled' != 2 "
                .to_string()
        )
    }
    Ok(())
}

fn make_tcp_listener(bind: &str, port: u16, backlog: libc::c_int) -> anyhow::Result<Fd> {
    let ipv4: Ipv4Addr = bind
        .parse()
        .context("Unable to parse ip address from bind arg")?;
    let ipv4_u32 = u32::from(ipv4).to_be();

    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("socket() failed");
        }

        let opt: libc::c_int = 1;
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &opt as *const _ as *const libc::c_void,
            std::mem::size_of_val(&opt) as libc::socklen_t,
        ) < 0
        {
            return Err(std::io::Error::last_os_error()).context("setsockopt(SO_REUSEPORT) failed");
        }

        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &opt as *const _ as *const libc::c_void,
            std::mem::size_of_val(&opt) as libc::socklen_t,
        ) < 0
        {
            return Err(std::io::Error::last_os_error()).context("setsockopt(SO_REUSEADDR) failed");
        }

        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: port.to_be(), // must be big-endian
            sin_addr: libc::in_addr { s_addr: ipv4_u32 },
            sin_zero: [0; 8],
        };
        if libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of_val(&addr) as libc::socklen_t,
        ) < 0
        {
            return Err(std::io::Error::last_os_error()).context("bind() failed");
        }

        if libc::listen(fd, backlog) < 0 {
            return Err(std::io::Error::last_os_error()).context("listen() failed");
        }

        Ok(Fd(fd))
    }
}
