use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use io_uring::{IoUring, opcode};

const PARAM_VALUES: &[usize] = &[1, 10, 100];

fn bench_pipe(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipe");

    for &calls_per_loop in PARAM_VALUES {
        group.bench_with_input(BenchmarkId::new("libc", calls_per_loop), &calls_per_loop, |b, &n| {
            b.iter(|| {
                for _ in 0..n {
                    let mut fds = [0i32; 2];
                    unsafe {
                        libc::pipe(fds.as_mut_ptr());
                        libc::close(fds[0]);
                        libc::close(fds[1]);
                    }
                    black_box(fds);
                }
            });
        });

        let mut ring = IoUring::new(calls_per_loop as u32).unwrap();

        group.bench_with_input(BenchmarkId::new("uring", calls_per_loop), &calls_per_loop, |b, &n| {
            b.iter(|| {
                let mut fds = vec![0i32; 2 * n];
                let mut fd_ptr = fds.as_mut_ptr();

                for _ in 0..n {
                    let entry = opcode::Pipe::new(fd_ptr).build();
                    unsafe {
                        ring.submission().push(&entry).expect("queue is full");
                    }
                    fd_ptr = unsafe { fd_ptr.add(2) };
                }

                ring.submit_and_wait(n).unwrap();
                ring.completion().map(black_box).for_each(drop);

                unsafe {
                    for fd in &fds {
                        if *fd > 0 {
                            libc::close(*fd);
                        }
                    }
                }
            });
        });
    }

    group.finish();
}

criterion_group!(pipe, bench_pipe);
criterion_main!(pipe);
