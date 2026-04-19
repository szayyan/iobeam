use io_uring::types::Fd;
use io_uring::{IoUring, SubmissionQueue, cqueue, opcode, squeue, types};
use slab::Slab;
use std::cmp::min;
use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::net::TcpListener;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};

const RW_BUF_SIZE: usize = 2048;
const MAX_CLIENT_HEADERS: usize = 32;

#[derive(Clone, Debug)]
enum Token {
    Accept,
    Poll {
        fd: RawFd,
    },
    Read {
        fd: RawFd,
        buf_index: usize,
        filled: usize,
    },
    Write {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
        file_fd: Option<(RawFd, usize)>,
    },
    Close,
}

unsafe fn queue_multishot_accept(
    fd: Fd,
    user_data: u64,
    sq: &mut SubmissionQueue,
    backlog: &mut VecDeque<squeue::Entry>,
) {
    let accept_entry = opcode::AcceptMulti::new(fd).build().user_data(user_data);
    unsafe {
        if sq.push(&accept_entry).is_err() {
            backlog.push_back(accept_entry);
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut ring = IoUring::new(256)?;
    let listener = TcpListener::bind(("127.0.0.1", 3456))?;
    let listener_fd = Fd(listener.as_raw_fd());

    let mut backlog = VecDeque::new();
    let mut bufpool = Vec::with_capacity(64);
    let mut buf_alloc = Slab::with_capacity(64);
    let mut token_alloc = Slab::with_capacity(64);

    println!("listen {}", listener.local_addr()?);

    let (submitter, mut sq, mut cq) = ring.split();

    unsafe {
        queue_multishot_accept(
            listener_fd,
            token_alloc.insert(Token::Accept) as _,
            &mut sq,
            &mut backlog,
        );
    }
    sq.sync();

    loop {
        match submitter.submit_and_wait(1) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
            Err(err) => return Err(err.into()),
        }
        cq.sync();

        // clean backlog
        loop {
            if sq.is_full() {
                match submitter.submit() {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
                    Err(err) => return Err(err.into()),
                }
            }
            sq.sync();

            match backlog.pop_front() {
                Some(sqe) => unsafe {
                    let _ = sq.push(&sqe);
                },
                None => break,
            }
        }

        for cqe in &mut cq {
            let ret = cqe.result();
            let flags = cqe.flags();
            let token_index = cqe.user_data() as usize;

            if ret < 0 {
                let token = token_alloc.get(token_index);

                eprintln!(
                    "token {:?} error: {:?}",
                    token,
                    io::Error::from_raw_os_error(-ret)
                );

                match token {
                    Some(Token::Accept) => unsafe {
                        queue_multishot_accept(
                            listener_fd,
                            token_index as _,
                            &mut sq,
                            &mut backlog,
                        );
                    },
                    Some(Token::Close) => {
                        // close has failed with either
                        // - EBADF — fd is not valid (double-close)
                        // - EINTR — interrupted by a signal
                        // in both cases the fd is already closed so remove from token_alloc and continue
                        token_alloc.remove(token_index);
                    }
                    _ => {}
                }
                continue;
            }

            let token = &mut token_alloc[token_index];
            match token.clone() {
                Token::Accept => {
                    println!("accept");

                    let fd = ret;
                    let poll_token = token_alloc.insert(Token::Poll { fd });

                    let poll_e = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as _)
                        .build()
                        .user_data(poll_token as _);

                    unsafe {
                        if sq.push(&poll_e).is_err() {
                            backlog.push_back(poll_e);
                        }
                    }

                    // If the multishot accept has ended, resubmit it.
                    if !cqueue::more(flags) {
                        unsafe {
                            queue_multishot_accept(
                                listener_fd,
                                token_index as _,
                                &mut sq,
                                &mut backlog,
                            );
                        }
                    }
                }
                Token::Poll { fd } => {
                    let (buf_index, buf) = match bufpool.pop() {
                        Some(buf_index) => (buf_index, &mut buf_alloc[buf_index]),
                        None => {
                            let buf = vec![0u8; RW_BUF_SIZE].into_boxed_slice();
                            let buf_entry = buf_alloc.vacant_entry();
                            let buf_index = buf_entry.key();
                            (buf_index, buf_entry.insert(buf))
                        }
                    };

                    *token = Token::Read {
                        fd,
                        buf_index,
                        filled: 0,
                    };

                    let read_e =
                        opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), RW_BUF_SIZE as _)
                            .build()
                            .user_data(token_index as _);

                    unsafe {
                        if sq.push(&read_e).is_err() {
                            backlog.push_back(read_e);
                        }
                    }
                }
                Token::Read {
                    fd,
                    buf_index,
                    filled,
                } => {
                    if ret == 0 {
                        bufpool.push(buf_index);

                        println!("shutdown");

                        *token = Token::Close;

                        let close_e = opcode::Close::new(types::Fd(fd))
                            .build()
                            .user_data(token_index as _);

                        unsafe {
                            if sq.push(&close_e).is_err() {
                                backlog.push_back(close_e);
                            }
                        }
                    } else {
                        let filled = filled + ret as usize;

                        let mut headers = [httparse::EMPTY_HEADER; MAX_CLIENT_HEADERS];
                        let mut req = httparse::Request::new(&mut headers);
                        let mut response: Option<&[u8]> = None;
                        let mut response_file: Option<(RawFd, usize)> = None;

                        match req.parse(&buf_alloc[buf_index][..filled]) {
                            Ok(httparse::Status::Complete(_)) => {
                                if let Some(method) = req.method
                                    && method.eq_ignore_ascii_case("get")
                                {
                                    match File::open("./public/index.html") {
                                        Ok(file) => {
                                            match file.metadata() {
                                                Ok(metadata) => {
                                                    let file_size = metadata.size() as usize;
                                                    let ffd = file.into_raw_fd(); // transfer ownership; fd won't be closed on drop
                                                    eprintln!(
                                                        "open ok: ffd={ffd} size={file_size}"
                                                    );
                                                    response = Some(b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n");
                                                    response_file = Some((ffd, file_size));
                                                }
                                                Err(e) => {
                                                    eprintln!("fstat error: {e}");
                                                    response = Some(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("open error: {e}");
                                            response = Some(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                        }
                                    }
                                } else {
                                    response = Some(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                }
                            }
                            Ok(httparse::Status::Partial) => {
                                if filled >= RW_BUF_SIZE {
                                    // Buffer exhausted with incomplete request — reject.
                                    response = Some(b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                } else {
                                    // Issue another recv into the remaining buffer space.

                                    let buf = &mut buf_alloc[buf_index];
                                    let next_ptr = unsafe { buf.as_mut_ptr().add(filled) };
                                    let remaining = RW_BUF_SIZE - filled;

                                    *token = Token::Read {
                                        fd,
                                        buf_index,
                                        filled,
                                    };

                                    let read_e =
                                        opcode::Recv::new(types::Fd(fd), next_ptr, remaining as _)
                                            .build()
                                            .user_data(token_index as _);

                                    unsafe {
                                        if sq.push(&read_e).is_err() {
                                            backlog.push_back(read_e);
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                response = Some(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            }
                        }

                        if let Some(response) = response {
                            let buf = &mut buf_alloc[buf_index];
                            let len = min(response.len(), RW_BUF_SIZE); // truncate response if buffer size exceeded
                            buf[..len].copy_from_slice(&response[..len]);

                            *token = Token::Write {
                                fd,
                                buf_index,
                                offset: 0,
                                len,
                                file_fd: response_file,
                            };

                            let write_e = opcode::Send::new(types::Fd(fd), buf.as_ptr(), len as _)
                                .build()
                                .user_data(token_index as _);

                            unsafe {
                                if sq.push(&write_e).is_err() {
                                    backlog.push_back(write_e);
                                }
                            }
                        }
                    }
                }
                Token::Write {
                    fd,
                    buf_index,
                    offset,
                    len,
                    file_fd,
                } => {
                    let write_len = ret as usize;

                    if offset + write_len >= len {
                        // Headers fully sent — send file body via sendfile if present, then close.
                        bufpool.push(buf_index);

                        if let Some((ffd, file_size)) = file_fd {
                            eprintln!("sendfile: fd={fd} ffd={ffd} size={file_size}");
                            let mut off: libc::off_t = 0;
                            let mut remaining = file_size;
                            while remaining > 0 {
                                let n = unsafe { libc::sendfile(fd, ffd, &mut off, remaining) };
                                eprintln!(
                                    "sendfile ret={n} remaining={remaining} errno={}",
                                    unsafe { *libc::__errno_location() }
                                );
                                if n <= 0 {
                                    break;
                                }
                                remaining -= n as usize;
                            }
                            unsafe {
                                libc::close(ffd);
                            }
                        }

                        println!("close");

                        *token = Token::Close;

                        let close_e = opcode::Close::new(types::Fd(fd))
                            .build()
                            .user_data(token_index as _);

                        unsafe {
                            if sq.push(&close_e).is_err() {
                                backlog.push_back(close_e);
                            }
                        }
                    } else {
                        // Partial write — send the remainder.
                        let offset = offset + write_len;
                        let len = len - offset;

                        let buf = &buf_alloc[buf_index][offset..];

                        *token = Token::Write {
                            fd,
                            buf_index,
                            offset,
                            len,
                            file_fd,
                        };

                        let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), len as _)
                            .build()
                            .user_data(token_index as _);

                        unsafe {
                            if sq.push(&entry).is_err() {
                                backlog.push_back(entry);
                            }
                        }
                    }
                }
                Token::Close { .. } => {
                    token_alloc.remove(token_index);
                }
            }
        }
    }
}
