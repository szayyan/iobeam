use clap::{Arg, Command, value_parser};

use crate::uring_server::WriteStrategy;

pub struct Cli {
    pub dir: String,
    pub host: String,
    pub port: u16,
    pub uring_entries: u32,
    pub body_write_chunk_size: usize,
    pub write_strategy: WriteStrategy,
}

impl Cli {
    pub fn parse() -> Self {
        let matches = Command::new("iobeam")
            .about("experimental io_uring HTTP file server")
            .arg(
                Arg::new("dir")
                    .help("Directory to serve files from")
                    .default_value("./public"),
            )
            .arg(
                Arg::new("host")
                    .long("host")
                    .help("Host address to listen on")
                    .default_value("127.0.0.1"),
            )
            .arg(
                Arg::new("port")
                    .short('p')
                    .long("port")
                    .help("Port to listen on")
                    .value_parser(value_parser!(u16))
                    .default_value("3456"),
            )
            .arg(
                Arg::new("uring_entries")
                    .long("uring-entries")
                    .help("io_uring submission queue entries")
                    .value_parser(value_parser!(u32))
                    .default_value("256"),
            )
            .arg(
                Arg::new("body_write_chunk_size")
                    .long("body-write-chunk-size")
                    .help("Body write chunk size in bytes")
                    .value_parser(value_parser!(usize))
                    .default_value("262144"),
            )
            .arg(
                Arg::new("write_strategy")
                    .long("write-strategy")
                    .help("Strategy used to write the response body")
                    .value_parser([
                        "send-file",
                        "send-file-in-async-event-loop",
                        "pipe-and-splice",
                    ])
                    .default_value("pipe-and-splice"),
            )
            .get_matches();

        let write_strategy = match matches
            .get_one::<String>("write_strategy")
            .unwrap()
            .as_str()
        {
            "send-file" => WriteStrategy::SendFile,
            "send-file-in-async-event-loop" => WriteStrategy::SendFileInAsyncEventLoop,
            _ => WriteStrategy::PipeAndSplice,
        };

        Self {
            dir: matches.get_one::<String>("dir").unwrap().clone(),
            host: matches.get_one::<String>("host").unwrap().clone(),
            port: *matches.get_one::<u16>("port").unwrap(),
            uring_entries: *matches.get_one::<u32>("uring_entries").unwrap(),
            body_write_chunk_size: *matches.get_one::<usize>("body_write_chunk_size").unwrap(),
            write_strategy,
        }
    }
}
