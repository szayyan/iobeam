# **iobeam**

An proof of concept HTTP file server built on Linux's `io_uring` interface.

Unlike many other server implementations iobeam does not make use of `Future`s or `async` and `await`. Instead it opts for a thread per core architecture with no shared state and delegates asynchronous scheduling to the kernel.

The performance is competitive with industry standard solutions. Benchmarks show a modest but measurable improvement over nginx for many workloads. See below for more detailed benchmarks.

#### **Requirements:** Linux kernel >= 5.19 (uses the `AcceptMulti` opcode)

---

## Usage

```sh
iobeam [DIR] [OPTIONS]
```

| Argument                  | Default                         | Description                       |
| ------------------------- | ------------------------------- | --------------------------------- |
| `DIR`                     | `./public`                      | Directory to serve files from     |
| `--bind`                  | `127.0.0.1`                     | Host address to listen on         |
| `-p, --port`              | `3456`                          | Port to listen on                 |
| `--threads`               | `4`                             | Number of worker threads          |
| `--write-strategy`        | `send-file-in-async-event-loop` | Body write strategy (see below)   |
| `--uring-entries`         | `256`                           | `io_uring` submission queue size  |
| `--body-write-chunk-size` | `262144`                        | Response body chunk size in bytes |
| `--backlog`               | `1024`                          | TCP listener backlog              |

### Write Strategies

Three strategies are available for sending the response body, selectable via `--write-strategy`:

- **`send-file-in-async-event-loop`** _(default)_ — `sendfile(2)` called inside the `io_uring` event loop, triggered via a no-op submission.
- **`pipe-and-splice`** — Zero-copy transfer using a pipe as an intermediary: `splice(file → pipe)` then `splice(pipe → socket)`, all driven by `io_uring`. _Note:_ This strategy is not currently competitive in terms of performance and requires several optimisations.
- **`send-file`** — Blocking `sendfile(2)` syscall called synchronously per request.

---

## Benchmarks

The `benchmarks/` directory contains tooling for performance testing:

- **`benchmarks/axum/`** — An equivalent Axum/Tokio file server for baseline comparison.
- **`benchmarks/micro/`** — A collection of small benchmarks for comparing uring and syscall equivalents.
- **`benchmarks/scripts/`** — Test data generator. Creates `small/` (100 files, up to 1 MB), `med/` (10 files, 1–100 MB), and `large/` (3 files, 1–2 GB) directories of random binary files.

---

## Results

Small file size workload w 256 connections

|                  | nginx         | iobeam        |
| :--------------- | :------------ | :------------ |
| Total Requests   | 136,182       | 144,133       |
| Total Data Read  | 62.82GB       | 66.48GB       |
| **Requests/sec** | **13,512.93** | **14,302.84** |
| **Transfer/sec** | **6.23GB**    | **6.60GB**    |
| Latency avg      | 17.92ms       | 13.17ms       |
| Latency stdev    | 3.54ms        | 19.65ms       |
| Latency max      | 233.80ms      | 588.14ms      |

Mixed file size workload w 256 connections

|                  | nginx        | iobeam       |
| :--------------- | :----------- | :----------- |
| Total Requests   | 14,806       | 15,256       |
| Total Data Read  | 64.69GB      | 65.83GB      |
| **Requests/sec** | **1,439.71** | **1,515.86** |
| **Transfer/sec** | **6.29GB**   | **6.54GB**   |
| Latency avg      | 259.44ms     | 264.56ms     |
| Latency stdev    | 399.25ms     | 407.34ms     |
| Latency max      | 1.99s        | 1.99ms       |

# Reproducing

1. Install [wrk](https://github.com/wg/wrk)
2. Run `cargo run -p scripts -- gen-test-data /var/www/files` to generate mock data
3. Run `./wrk -c256 -t4 -s./requests.lua http://127.0.0.1:3456 -- small` to measure small file tranfer. the script also accepts med and large and any combination of the above. requests.lua can be found in `benchmarks/scripts/lua`
4. Run nginx with the configuration in `benchmarks/nginx.conf` and rerun wrk pointed to nginx's port

---

### TODO

- [ ] Use registered buffers
- [ ] SQPOLL support
- [ ] Improvements to pipe and splice strategy. Reuse pipes and chain splice commands instead of buffering entire file
- [ ] Optimise buffer pool usage on connection reuse
- [ ] kTLS research + integration
- [ ] HTTP 2/3 support
