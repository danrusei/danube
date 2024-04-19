mod service;

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();

    // run the HTTP server
    service::run();
}
