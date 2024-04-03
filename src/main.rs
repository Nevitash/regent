use kafka::create_topic;

mod kafka;

#[tokio::main]
async fn main() {
    create_topic("localhost:9092", "test_group", "test_client", "test_topic").await.expect("Can't create topic");
}
