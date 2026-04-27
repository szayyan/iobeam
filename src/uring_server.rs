use crate::{
    buffer_pool::{BUFFER_POOL_ITEM_SIZE, BufferPool},
    file_system::{FileResult, FileSystemHandler},
    http::{
        HttpHeaderBuffer, decode_http_request_path, is_get_request, write_dynamic_ok_response,
        write_static_bad_request_error, write_static_content_not_found_error,
        write_static_content_too_large_error, write_static_internal_server_error,
    },
};
use io_uring::{
    CompletionQueue, IoUring, SubmissionQueue, Submitter, cqueue, opcode, squeue::Entry, types::Fd,
};
use slab::Slab;
use std::{
    collections::VecDeque,
    io::{self, ErrorKind},
    os::fd::RawFd,
    path::Path,
};

#[derive(Copy, Clone, clap::ValueEnum)]
pub enum WriteStrategy {
    SendFile,
    SendFileInAsyncEventLoop,
    PipeAndSplice,
}

pub struct UringServer<'a> {
    core: UringCore<'a>,
    cq: CompletionQueue<'a>,
}

struct UringCore<'a> {
    listener: Fd,
    write_strategy: WriteStrategy,
    file_system_handler: FileSystemHandler,
    usi: UringSubmissionInterface<'a>,
    buffer_pool: BufferPool,
    token_alloc: Slab<Token>,
    body_write_chunk_size: usize,
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
            self.queue_multishot_accept(accept_fd, accept_user_data);
        }
    }

    fn queue_close(&mut self, fd: Fd) {
        let close_entry = opcode::Close::new(fd).build().user_data(NO_USER_DATA);

        unsafe {
            if self.sq.push(&close_entry).is_err() {
                self.backlog.push_back(close_entry);
            }
        }
    }

    fn queue_noop(&mut self, user_data: u64) {
        let noop_entry = opcode::Nop::new().build().user_data(user_data);
        unsafe {
            if self.sq.push(&noop_entry).is_err() {
                self.backlog.push_back(noop_entry);
            }
        }
    }

    fn queue_splice(
        &mut self,
        fd_in: Fd,
        off_in: i64,
        fd_out: Fd,
        off_out: i64,
        len: u32,
        flags: u32,
        user_data: u64,
    ) {
        let splice_entry = opcode::Splice::new(fd_in, off_in, fd_out, off_out, len)
            .flags(flags)
            .build()
            .user_data(user_data);
        unsafe {
            if self.sq.push(&splice_entry).is_err() {
                self.backlog.push_back(splice_entry);
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
        write_strategy: WriteStrategy,
        file_system_handler: FileSystemHandler,
        submitter: Submitter<'a>,
        sq: SubmissionQueue<'a>,
        buffer_pool: BufferPool,
        token_alloc: Slab<Token>,
        body_write_chunk_size: usize,
    ) -> Self {
        Self {
            listener,
            write_strategy,
            file_system_handler,
            usi: UringSubmissionInterface::new(submitter, sq),
            buffer_pool,
            token_alloc,
            body_write_chunk_size,
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
            Token::WriteBodySendFile {
                fd,
                body_fd,
                offset,
                len,
            } => self.handle_write_body_token(fd, body_fd, offset, len, token_index),
            Token::WriteBodySpliceFileToPipe {
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                file_offset,
                remaining,
            } => self.handle_splice_file_to_pipe_token(
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                file_offset,
                remaining,
                ret,
                token_index,
            ),
            Token::WriteBodySplicePipeToSock {
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                file_offset,
                remaining,
                in_pipe,
            } => self.handle_splice_pipe_to_sock_token(
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                file_offset,
                remaining,
                in_pipe,
                ret,
                token_index,
            ),
        }
    }

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
            self.token_alloc.remove(token_index);
            self.usi.queue_close(Fd(fd));
            debug!("no data read from client - closing connection");
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
                    let raw_path = request.path.unwrap_or("");
                    let path_buffer = decode_http_request_path(raw_path).and_then(|v| {
                        self.file_system_handler
                            .construct_and_validate_decoded_path(Path::new(&*v))
                    });
                    if let Ok(path) = path_buffer {
                        let path = path.as_str();
                        debug!("client requested path {}", path);
                        match self.file_system_handler.open_raw_ffd(path) {
                            Ok(result) => {
                                debug!("file found - successful request");
                                write_dynamic_ok_response(
                                    &mut response_header_buffer,
                                    path,
                                    result.size,
                                );
                                response_body = Some(result)
                            }
                            Err(e)
                                if e.kind() == ErrorKind::NotFound
                                    || e.kind() == ErrorKind::IsADirectory =>
                            {
                                debug!("file not found - unsuccessful request");
                                write_static_content_not_found_error(&mut response_header_buffer);
                            }
                            Err(e) => {
                                error!("error opening path {} {:?}", path, e);
                                write_static_internal_server_error(&mut response_header_buffer)
                            }
                        }
                    } else {
                        debug!("client requested invalid path");
                        write_static_bad_request_error(&mut response_header_buffer);
                    };
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

    fn begin_write_body(&mut self, token_index: usize, fd: RawFd, body: FileResult) {
        match self.write_strategy {
            WriteStrategy::SendFile => {
                let mut off: libc::off_t = 0;
                let mut remaining = body.size;
                while remaining > 0 {
                    let n = unsafe {
                        libc::sendfile(
                            fd,
                            body.fd,
                            &mut off,
                            remaining.min(self.body_write_chunk_size),
                        )
                    };
                    if n <= 0 {
                        break;
                    }
                    remaining -= n as usize;
                }

                // TODO: reuse buffer directly instead of removing + pushing
                // will need to pass buf_index but must be valid
                // self.token_alloc.remove(token_index);
                self.usi.queue_close(Fd(body.fd));
                self.handle_poll_token(fd, token_index); // reuse token
                // self.usi.queue_close(Fd(fd));
                // debug!("closing connection");
            }
            WriteStrategy::SendFileInAsyncEventLoop => {
                self.token_alloc[token_index] = Token::WriteBodySendFile {
                    fd,
                    body_fd: body.fd,
                    offset: 0,
                    len: body.size,
                };
                self.usi.queue_noop(token_index as _);
            }
            WriteStrategy::PipeAndSplice => {
                let mut pipe_fds = [-1i32; 2];
                unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) };
                let chunk = body.size.min(self.body_write_chunk_size) as u32;
                self.token_alloc[token_index] = Token::WriteBodySpliceFileToPipe {
                    fd,
                    body_fd: body.fd,
                    pipe_read: pipe_fds[0],
                    pipe_write: pipe_fds[1],
                    file_offset: 0,
                    remaining: body.size,
                };
                self.usi.queue_splice(
                    Fd(body.fd),
                    0,
                    Fd(pipe_fds[1]),
                    -1,
                    chunk,
                    libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE,
                    token_index as _,
                );
            }
        }
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
            // TODO: reuse connection instead of close
            self.buffer_pool.return_to_pool(buf_index);
            if let Some(body) = body {
                self.begin_write_body(token_index, fd, body);
            } else {
                self.token_alloc.remove(token_index);
                self.usi.queue_close(Fd(fd));
                debug!("closing connection");
            }
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

    fn close_body_and_connection(&mut self, fd: RawFd, body_fd: Fd, token_index: usize) {
        debug!("closing connection");
        // self.token_alloc.remove(token_index);
        // self.usi.queue_close(fd);
        self.usi.queue_close(body_fd);
        self.handle_poll_token(fd, token_index);
    }

    fn handle_write_body_token(
        &mut self,
        fd: RawFd,
        body_fd: RawFd,
        offset: libc::off_t,
        len: usize,
        token_index: usize,
    ) {
        let chunk_size = (len - offset as usize).min(self.body_write_chunk_size);

        if chunk_size <= 0 {
            self.close_body_and_connection(fd, Fd(body_fd), token_index);
            return;
        }

        let mut off: libc::off_t = offset;
        let n = unsafe { libc::sendfile(fd, body_fd, &mut off, chunk_size) };
        let remaining = len as isize - n;
        if n <= 0 || remaining <= 0 {
            self.close_body_and_connection(fd, Fd(body_fd), token_index);
            return;
        }

        self.token_alloc[token_index] = Token::WriteBodySendFile {
            fd,
            body_fd,
            offset: off,
            len: remaining as _,
        }
    }

    fn handle_splice_file_to_pipe_token(
        &mut self,
        fd: RawFd,
        body_fd: RawFd,
        pipe_read: RawFd,
        pipe_write: RawFd,
        file_offset: libc::off_t,
        remaining: usize,
        ret: i32,
        token_index: usize,
    ) {
        let spliced = ret as usize;
        let new_file_offset = file_offset + spliced as libc::off_t;
        let new_remaining = remaining - spliced;
        let flags = libc::SPLICE_F_MOVE
            | if new_remaining > 0 {
                libc::SPLICE_F_MORE
            } else {
                0
            };
        self.token_alloc[token_index] = Token::WriteBodySplicePipeToSock {
            fd,
            body_fd,
            pipe_read,
            pipe_write,
            file_offset: new_file_offset,
            remaining: new_remaining,
            in_pipe: spliced,
        };
        self.usi.queue_splice(
            Fd(pipe_read),
            -1,
            Fd(fd),
            -1,
            spliced as u32,
            flags,
            token_index as _,
        );
    }

    fn handle_splice_pipe_to_sock_token(
        &mut self,
        fd: RawFd,
        body_fd: RawFd,
        pipe_read: RawFd,
        pipe_write: RawFd,
        file_offset: i64,
        remaining: usize,
        in_pipe: usize,
        ret: i32,
        token_index: usize,
    ) {
        let sent = ret as usize;
        let still_in_pipe = in_pipe - sent;

        if still_in_pipe > 0 {
            // Partial splice to socket — drain remaining bytes in the pipe first.
            let flags = libc::SPLICE_F_MOVE
                | if remaining > 0 {
                    libc::SPLICE_F_MORE
                } else {
                    0
                };
            self.token_alloc[token_index] = Token::WriteBodySplicePipeToSock {
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                file_offset,
                remaining,
                in_pipe: still_in_pipe,
            };
            self.usi.queue_splice(
                Fd(pipe_read),
                -1,
                Fd(fd),
                -1,
                still_in_pipe as u32,
                flags,
                token_index as _,
            );
        } else if remaining > 0 {
            // Pipe drained — splice next chunk from file into pipe.
            let chunk = remaining.min(self.body_write_chunk_size) as u32;
            self.token_alloc[token_index] = Token::WriteBodySpliceFileToPipe {
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                file_offset,
                remaining,
            };
            self.usi.queue_splice(
                Fd(body_fd),
                file_offset,
                Fd(pipe_write),
                -1,
                chunk,
                libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE,
                token_index as _,
            );
        } else {
            // All data sent — close pipe fds and the file fd, then close the socket.
            // self.token_alloc.remove(token_index);
            // self.usi.queue_close(Fd(fd));
            self.usi.queue_close(Fd(pipe_read));
            self.usi.queue_close(Fd(pipe_write));
            self.usi.queue_close(Fd(body_fd));

            self.handle_poll_token(fd, token_index);

            debug!("closing client connection");
        }
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
        debug!("connection accepted");

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
        let error = io::Error::from_raw_os_error(-ret);
        let token = self.token_alloc.get(token_index);

        error!(
            "token {:?} error: {:?}",
            self.token_alloc.get(token_index),
            error
        );

        match token {
            None => return,
            Some(Token::Accept) => {
                debug!("rearming accept");
                self.usi
                    .queue_multishot_accept(self.listener, token_index as _);
                return;
            }
            Some(Token::WriteBodySendFile { fd, body_fd, .. }) => {
                error!("closing client connection");
                self.usi.queue_close(Fd(*body_fd));
                self.usi.queue_close(Fd(*fd));
            }
            Some(Token::WriteBodySpliceFileToPipe {
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                ..
            }) => {
                error!("closing client connection");
                self.usi.queue_close(Fd(*fd));
                self.usi.queue_close(Fd(*pipe_read));
                self.usi.queue_close(Fd(*pipe_write));
                self.usi.queue_close(Fd(*body_fd));
            }
            Some(Token::WriteBodySplicePipeToSock {
                fd,
                body_fd,
                pipe_read,
                pipe_write,
                ..
            }) => {
                error!("closing client connection");
                self.usi.queue_close(Fd(*fd));
                self.usi.queue_close(Fd(*pipe_read));
                self.usi.queue_close(Fd(*pipe_write));
                self.usi.queue_close(Fd(*body_fd));
            }
            // TODO: consider how to handle other event failures
            _ => {}
        }
        self.token_alloc.remove(token_index);
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
        write_strategy: WriteStrategy,
        file_system_handler: FileSystemHandler,
        ring: &'a mut IoUring,
        buffer_pool: BufferPool,
        token_alloc: Slab<Token>,
        body_write_chunk_size: usize,
    ) -> UringServer<'a> {
        let (submitter, sq, cq) = ring.split();

        Self {
            core: UringCore::new(
                listener,
                write_strategy,
                file_system_handler,
                submitter,
                sq,
                buffer_pool,
                token_alloc,
                body_write_chunk_size,
            ),
            cq,
        }
    }

    pub fn start_event_loop(&mut self) -> anyhow::Result<()> {
        self.core.prep_initial_accept();

        loop {
            self.core.usi.submit_and_wait()?; // NOTE ing here may leave fd's open
            self.cq.sync();
            self.core.usi.sync_sq_and_empty_backlog()?;

            for cqe in &mut self.cq {
                let ret = cqe.result();
                let flags = cqe.flags();
                let user_data = cqe.user_data();

                if user_data == NO_USER_DATA {
                    continue;
                } else if ret < 0 {
                    self.core.handle_error_event(ret, user_data as _);
                } else {
                    self.core.handle_event(ret, user_data as _, flags);
                }
            }
        }
    }
}

const NO_USER_DATA: u64 = u64::MAX;

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
    // todo: investigate why so slow? noop related?
    WriteBodySendFile {
        fd: RawFd,
        body_fd: RawFd,
        offset: libc::off_t,
        len: usize,
    },
    WriteBodySpliceFileToPipe {
        fd: RawFd,
        body_fd: RawFd,
        pipe_read: RawFd,
        pipe_write: RawFd,
        file_offset: libc::off_t,
        remaining: usize,
    },
    WriteBodySplicePipeToSock {
        fd: RawFd,
        body_fd: RawFd,
        pipe_read: RawFd,
        pipe_write: RawFd,
        file_offset: libc::off_t,
        remaining: usize,
        in_pipe: usize,
    },
}
