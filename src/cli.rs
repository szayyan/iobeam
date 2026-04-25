use crate::uring_server::WriteStrategy;
use clap::Parser;

#[derive(Parser)]
#[command(about = "experimental io_uring HTTP file server")]
pub struct Cli {
    /// Directory to serve files from
    #[arg(default_value = "./public")]
    pub dir: String,

    /// Host address to listen on
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: String,

    /// Port to listen on
    #[arg(short, long, default_value_t = 3456)]
    pub port: u16,

    /// io_uring submission queue entries
    #[arg(long, default_value_t = 256)]
    pub uring_entries: u32,

    /// Body write chunk size in bytes
    #[arg(long, default_value_t = 262144)]
    pub body_write_chunk_size: usize,

    /// Strategy used to write the response body
    #[arg(long, default_value = "pipe-and-splice")]
    pub write_strategy: WriteStrategy,

    /// TCP listener backlog
    #[arg(long, default_value_t = 1024)]
    pub backlog: i32,
}

impl Cli {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
