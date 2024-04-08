use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::FromClientConfig,
    error::{KafkaError},
    ClientConfig,
};
use log::{info, error};

pub async fn create_topic(
    broker_string: &str,
    group_id: &str,
    client_id: &str,
    topic: &str,
) -> Result<(), KafkaError> {
    let replication = broker_string.split(",").count();
    let client = create_admin_client(broker_string, group_id, client_id).await?;
    let topic = NewTopic::new(topic, 10, TopicReplication::Fixed(replication.try_into().unwrap()));
    let results = client
        .create_topics(vec![&topic], &AdminOptions::new())
        .await?;
    for result in results {
        match result {
            Ok(_) => info!("Topic created successfully"),
            Err((_, e)) => {
                error!("Failed to create topic: {}", e);
                return Err(KafkaError::AdminOpCreation(format!("Failed to create topic: {}", error_message)));
            }
        }
    }
    Ok(())
}

async fn create_admin_client(
    broker_string: &str,
    group_id: &str,
    client_id: &str,
) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    let mut config: ClientConfig = ClientConfig::new();
    config.set(KafkaConfigProperty::BOOTSTRAP_SERVERS, broker_string);
    config.set(KafkaConfigProperty::GROUP_ID, group_id);
    config.set(KafkaConfigProperty::CLIENT_ID, client_id);
    config.set(KafkaConfigProperty::MESSAGE_TIMEOUT_MS, "60000");
    config.set(KafkaConfigProperty::ACKS, "all");
    AdminClient::from_config(&config)
}

struct KafkaConfigProperty;
impl KafkaConfigProperty {
    pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
    pub const CLIENT_ID: &str = "client.id";
    pub const MESSAGE_TIMEOUT_MS: &str = "message.timeout.ms";
    pub const ENABLE_AUTO_COMMIT: &str = "enable.auto.commit";
    pub const AUTO_OFFSET_COMMIT: &str = "auto.offset.reset";
    pub const GROUP_ID: &str = "group.id";
    pub const MAX_POLL_RECORDS: &str = "max.poll.records";
    pub const ACKS: &str = "acks";
}
