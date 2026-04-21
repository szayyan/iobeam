use arrayvec::ArrayVec;
use io_uring::{
    CompletionQueue, IoUring, SubmissionQueue, Submitter, cqueue, opcode, squeue::Entry, types::Fd,
};
use slab::Slab;
use std::{
    collections::VecDeque,
    io::{self, ErrorKind},
    os::fd::RawFd,
};

use crate::{
    buffer_pool::{self, BUFFER_POOL_ITEM_SIZE, BufferPool},
    file_system::{FileResult, FileSystemHandler},
    http::{
        HttpHeaderBuffer, is_get_request, write_static_bad_request_error,
        write_static_content_not_found_error, write_static_content_too_large_error,
        write_static_internal_server_error, write_static_ok_response,
    },
};

pub struct UringServer<'a> {
    core: UringCore<'a>,
    cq: CompletionQueue<'a>,
}

struct UringCore<'a> {
    listener: Fd,
    file_system_handler: FileSystemHandler,
    usi: UringSubmissionInterface<'a>,
    buffer_pool: BufferPool,
    token_alloc: Slab<Token>,
}

struct UringSubmissionInterface<'a> {
    submitter: Submitter<'a>,
    sq: SubmissionQueue<'a>,
    backlog: VecDeque<Entry>,
}

impl<'a> UringSubmissionInterface<'a> {
    fn new(submitter: Submitter<'a>, sq: SubmissionQueue<'a>) -> Self {
        Self {
            submitter,
            sq,
            backlog: VecDeque::new(),
        }
    }

    fn submit_and_wait(&self) -> io::Result<()> {
        match self.submitter.submit_and_wait(1) {
            Ok(_) => Ok(()),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => Ok(()),
            Err(err) => return Err(err.into()),
        }
    }

    fn sync_queue(&mut self) {
        self.sq.sync();
    }

    fn queue_send(&mut self, fd: Fd, buf: *const u8, len: usize, user_data: u64) {
        let write_entry = opcode::Send::new(fd, buf, len as _)
            .build()
            .user_data(user_data);

        unsafe {
            if self.sq.push(&write_entry).is_err() {
                self.backlog.push_back(write_entry);
            }
        }
    }

    fn queue_multishot_accept(&mut self, fd: Fd, user_data: u64) {
        let accept_entry = opcode::AcceptMulti::new(fd).build().user_data(user_data);
        unsafe {
            if self.sq.push(&accept_entry).is_err() {
                self.backlog.push_back(accept_entry);
            }
        }
    }

    fn queue_recv(&mut self, fd: Fd, buf: *mut u8, len: usize, user_data: u64) {
        let read_entry = opcode::Recv::new(fd, buf, len as _)
            .build()
            .user_data(user_data);

        unsafe {
            if self.sq.push(&read_entry).is_err() {
                self.backlog.push_back(read_entry);
            }
        }
    }

    fn queue_poll_add(
        &mut self,
        poll_fd: Fd,
        poll_user_data: u64,
        accept_fd: Fd,
        accept_user_data: u64,
        flags: u32,
    ) {
        let poll_entry = opcode::PollAdd::new(poll_fd, libc::POLLIN as _)
            .build()
            .user_data(poll_user_data);

        unsafe {
            if self.sq.push(&poll_entry).is_err() {
                self.backlog.push_back(poll_entry);
            }
        }

        // If the multishot accept has ended, resubmit it.
        if !cqueue::more(flags) {
            unsafe {
                self.queue_multishot_accept(accept_fd, accept_user_data);
            }
        }
    }

    fn queue_close(&mut self, fd: Fd, user_data: u64) {
        let close_entry = opcode::Close::new(fd).build().user_data(user_data);

        unsafe {
            if self.sq.push(&close_entry).is_err() {
                self.backlog.push_back(close_entry);
            }
        }
    }

    fn sync_sq_and_empty_backlog(&mut self) -> io::Result<()> {
        loop {
            if self.sq.is_full() {
                match self.submitter.submit() {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => return Ok(()),
                    Err(err) => return Err(err.into()),
                }
            }
            self.sq.sync();

            match self.backlog.pop_front() {
                Some(sqe) => unsafe {
                    _ = self.sq.push(&sqe);
                },
                None => return Ok(()),
            }
        }
    }
}

impl<'a> UringCore<'a> {
    fn new(
        listener: Fd,
        file_system_handler: FileSystemHandler,
        submitter: Submitter<'a>,
        sq: SubmissionQueue<'a>,
        buffer_pool: BufferPool,
        token_alloc: Slab<Token>,
    ) -> Self {
        Self {
            listener,
            file_system_handler,
            usi: UringSubmissionInterface::new(submitter, sq),
            buffer_pool,
            token_alloc,
        }
    }

    fn handle_event(&mut self, ret: i32, token_index: usize, flags: u32) {
        // TODO: test performance of clone() vs &self.token_alloc[..] and derefencing enum properties
        match self.token_alloc[token_index].clone() {
            Token::Accept => self.handle_accept_token(ret, token_index, flags),
            Token::Poll { fd } => self.handle_poll_token(fd, token_index),
            Token::Read {
                fd,
                buf_index,
                filled,
            } => self.handle_read_token(fd, buf_index, filled, ret, token_index),
            Token::WriteHeaders {
                fd,
                buf_index,
                offset,
                len,
                body,
            } => {
                self.handle_write_headers_token(fd, buf_index, offset, len, body, ret, token_index)
            }
            Token::WriteBody { fd, offset, len } => self.handle_write_body_token(fd, offset, len),
            Token::Close => self.handle_close_token(token_index),
        }
    }

    // TODO: performance test - better to write directly to heap buffer or accumulate in stack and one copy straight to heap
    fn handle_read_token(
        &mut self,
        fd: RawFd,
        buf_index: usize,
        filled: usize,
        ret: i32,
        token_index: usize,
    ) {
        if ret == 0 {
            self.buffer_pool.return_to_pool(buf_index);
            self.token_alloc[token_index] = Token::Close;
            self.usi.queue_close(Fd(fd), token_index as _);
            return;
        }

        let filled = filled + ret as usize;
        let mut request_headers = [httparse::EMPTY_HEADER; 16];
        let mut request = httparse::Request::new(&mut request_headers);
        let result = request.parse(&self.buffer_pool.get(buf_index)[..filled]);

        let mut response_header_buffer = HttpHeaderBuffer::new();
        let mut response_body: Option<FileResult> = None;

        match result {
            Ok(httparse::Status::Complete(_)) => {
                if !is_get_request(&request) {
                    write_static_bad_request_error(&mut response_header_buffer);
                } else {
                    match self.file_system_handler.open_raw_fd("./public/index.html") {
                        Ok(result) => {
                            write_static_ok_response(&mut response_header_buffer);
                            response_body = Some(result)
                        }
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            write_static_content_not_found_error(&mut response_header_buffer);
                        }
                        Err(e) => write_static_internal_server_error(&mut response_header_buffer),
                    }
                }
            }
            Ok(httparse::Status::Partial) => {
                if filled >= BUFFER_POOL_ITEM_SIZE {
                    write_static_content_too_large_error(&mut response_header_buffer);
                } else {
                    // Issue another recv into the remaining buffer space.
                    let buf = self.buffer_pool.get_mut(buf_index);
                    let next_ptr = unsafe { buf.as_mut_ptr().add(filled) };
                    let remaining = BUFFER_POOL_ITEM_SIZE - filled;
                    self.token_alloc[token_index] = Token::Read {
                        fd,
                        buf_index,
                        filled,
                    };
                    self.usi
                        .queue_recv(Fd(fd), next_ptr, remaining as _, token_index as _);
                    return;
                }
            }
            Err(_) => write_static_internal_server_error(&mut response_header_buffer),
        }

        let header_bytes = response_header_buffer.as_slice();
        let header_len = header_bytes.len();

        let buf = self.buffer_pool.get_mut(buf_index);
        buf[..header_len].copy_from_slice(header_bytes);

        self.token_alloc[token_index] = Token::WriteHeaders {
            fd,
            buf_index,
            offset: 0,
            len: header_len,
            body: response_body,
        };

        self.usi
            .queue_send(Fd(fd), buf.as_ptr(), header_len, token_index as _);
    }

    fn handle_write_headers_token(
        &mut self,
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
        body: Option<FileResult>,
        ret: i32,
        token_index: usize,
    ) {
        let write_len = ret as usize;
        let write_complete = offset + write_len >= len;

        if write_complete {
            self.buffer_pool.return_to_pool(buf_index);

            if let Some(body) = body {
                let mut off: libc::off_t = 0;
                let mut remaining = body.size;
                while remaining > 0 {
                    let n = unsafe { libc::sendfile(fd, body.fd, &mut off, remaining) };
                    if n <= 0 {
                        break;
                    }
                    remaining -= n as usize;
                }
                unsafe {
                    libc::close(body.fd);
                }
            }

            self.token_alloc[token_index] = Token::Close;
            self.usi.queue_close(Fd(fd), token_index as _);
        } else {
            // Partial write — send the remainder.
            let offset = offset + write_len;
            let len = len - offset;

            let buf = &self.buffer_pool.get(buf_index)[offset..];

            self.token_alloc[token_index] = Token::WriteHeaders {
                fd,
                buf_index,
                offset,
                len,
                body,
            };

            self.usi
                .queue_send(Fd(fd), buf.as_ptr(), len, token_index as _);
        }
    }

    fn handle_write_body_token(&mut self, _fd: RawFd, _offset: usize, _len: usize) {
        unimplemented!()
    }

    fn handle_close_token(&mut self, token_index: usize) {
        println!("connection CLOSED");
        self.token_alloc.remove(token_index);
    }

    fn handle_poll_token(&mut self, fd: RawFd, token_index: usize) {
        let (buf_index, buf) = self.buffer_pool.reuse_or_allocate();
        self.token_alloc[token_index] = Token::Read {
            fd,
            buf_index,
            filled: 0,
        };
        self.usi.queue_recv(
            Fd(fd),
            buf.as_mut_ptr(),
            BUFFER_POOL_ITEM_SIZE,
            token_index as _,
        );
    }

    fn handle_accept_token(&mut self, ret: i32, token_index: usize, flags: u32) {
        println!("connection accepted");

        let fd = ret;
        let poll_token = self.token_alloc.insert(Token::Poll { fd });

        self.usi.queue_poll_add(
            Fd(fd),
            poll_token as _,
            self.listener,
            token_index as _,
            flags,
        );
    }

    fn handle_error_event(&mut self, ret: i32, token_index: usize) {
        let token = self.token_alloc.get(token_index);
        let error = io::Error::from_raw_os_error(-ret);

        eprintln!("token {:?} error: {:?}", token, error);

        match token {
            Some(Token::Accept) => {
                self.usi
                    .queue_multishot_accept(self.listener, token_index as _);
            }
            Some(Token::Close) => {
                // close has failed with either
                // - EBADF — fd is not valid (double-close)
                // - EINTR — interrupted by a signal
                // in both cases the fd is already closed so remove from token_alloc and continue
                self.token_alloc.remove(token_index);
            }
            // TODO: consider how to handle other event failures
            _ => {}
        }
    }

    fn prep_initial_accept(&mut self) {
        let accept_token_key = self.token_alloc.insert(Token::Accept);
        self.usi
            .queue_multishot_accept(self.listener, accept_token_key as _);
        self.usi.sync_queue();
    }
}
impl<'a> UringServer<'a> {
    pub fn new(
        listener: Fd,
        file_system_handler: FileSystemHandler,
        ring: &'a mut IoUring,
        buffer_pool: BufferPool,
        token_alloc: Slab<Token>,
    ) -> UringServer<'a> {
        let (submitter, sq, cq) = ring.split();

        Self {
            core: UringCore::new(
                listener,
                file_system_handler,
                submitter,
                sq,
                buffer_pool,
                token_alloc,
            ),
            cq,
        }
    }

    pub fn start_event_loop(&mut self) -> anyhow::Result<()> {
        self.core.prep_initial_accept();

        loop {
            self.core.usi.submit_and_wait()?; // NOTE erroring here may leave fd's open leaking memory
            self.cq.sync();
            self.core.usi.sync_sq_and_empty_backlog()?;

            for cqe in &mut self.cq {
                let ret = cqe.result();
                let flags = cqe.flags();
                let token_index = cqe.user_data() as usize;

                if ret < 0 {
                    self.core.handle_error_event(ret, token_index);
                } else {
                    self.core.handle_event(ret, token_index, flags);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Token {
    Accept,
    Poll {
        fd: RawFd,
    },
    Read {
        fd: RawFd,
        buf_index: usize,
        filled: usize,
    },
    WriteHeaders {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
        body: Option<FileResult>,
    },
    WriteBody {
        fd: RawFd,
        offset: usize,
        len: usize,
    },
    Close,
}
