use io_uring::{CompletionQueue, IoUring, SubmissionQueue, Submitter, squeue::Entry};
use slab::Slab;
use std::{collections::VecDeque, os::fd::RawFd};

struct UringServer<'a> {
    ring: IoUring,
    submitter: Submitter<'a>,
    sq: SubmissionQueue<'a>,
    cq: CompletionQueue<'a>,
    backlog: VecDeque<Entry>,
    bufpool: Vec<usize>,
    buf_alloc: Slab<Box<[u8]>>,
    token_alloc: Slab<Token>,
}

impl UringServer<'_> {
    #[inline]
    fn return_buffer_to_pool(&mut self, index: usize) {
        self.bufpool.push(index);
    }
}

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
    WriteHeaders {
        fd: RawFd,
        buf_index: usize,
        len: usize,
        body: Option<(RawFd, usize)>,
    },
    WriteBody {
        fd: RawFd,
        offset: usize,
        len: usize,
    },
    Close,
}
