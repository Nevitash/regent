use kafka::create_topic;
use log::{info, LevelFilter};
use log4rs::{append::console::ConsoleAppender, config::{Appender, Logger, Root}, Config};
use grpc::server::{kafka_service::kafka_service_server::{KafkaServiceServer}, KafkaServiceImpl};
use tonic::transport::Server;

mod kafka;
mod grpc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging().await;
    init_grpc_server_kafka().await?;
    Ok(())
}


async fn init_grpc_server_kafka() -> Result<(), Box<dyn std::error::Error>> {
    let url = "[::0]:50051".parse()?;
    let grpc_server = KafkaServiceImpl::default();

    Server::builder()
        .add_service(KafkaServiceServer::new(grpc_server))
        .serve(url)
        .await?;

    Ok(())
}

async fn init_logging() {
    let stdout = ConsoleAppender::builder().build();
    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .logger(Logger::builder().build("sqlx", LevelFilter::Warn))
        .build(Root::builder().appender("stdout").build(LevelFilter::Trace))
        .unwrap();
    log4rs::init_config(config).unwrap();
    info!("log4rs is initialized, ready to log!");
}
