use kafka::create_topic;
use log::{info, LevelFilter};
use log4rs::{append::console::ConsoleAppender, config::{Appender, Logger, Root}, Config};

mod kafka;

#[tokio::main]
async fn main(){
    init_logging().await;
    create_topic("kafka:9092,kafka2:9092", "test_group", "test_client", "test_topic5").await.expect("Can't create topic");
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
