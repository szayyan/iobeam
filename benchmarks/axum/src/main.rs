use axum::Router;
use clap::Parser;
use std::net::SocketAddr;
use tower_http::services::ServeDir;

#[derive(Parser)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Directory to serve
    #[arg(short, long, default_value = ".")]
    dir: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let app = Router::new().fallback_service(ServeDir::new(&args.dir));

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("Serving {} on http://{}", args.dir, addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
