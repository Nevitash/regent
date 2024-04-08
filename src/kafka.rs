use log::{error, info};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::FromClientConfig,
    error::KafkaError,
    types::RDKafkaErrorCode,
    ClientConfig,
};

pub async fn create_topic(
    broker_string: &str,
    group_id: &str,
    client_id: &str,
    topic_name: &str,
) -> Result<(), KafkaError> {
    let replication = broker_string.split(",").count();
    let topic = NewTopic::new(
        topic_name,
        1,
        TopicReplication::Fixed(replication.try_into().unwrap()),
    );
    let client = create_admin_client(broker_string, group_id, client_id).await?;
    
    let results = client
        .create_topics(vec![&topic], &AdminOptions::new())
        .await?;
    for result in results {
        match result {
            Ok(_) => info!("Topic '{}' created successfully", topic_name),
            Err((_, e)) => match e {
                RDKafkaErrorCode::TopicAlreadyExists => info!("Topic {} already exists", &topic_name),
                _ => {
                    error!("Failed to create topic '{}': {}", topic_name, e);
                    let error_message = format!("Failed to create topic: {}", &topic_name);
                    return Err(KafkaError::AdminOpCreation(error_message));
                }
            },
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
    pub const BOOTSTRAP_SERVERS: &'static str = "bootstrap.servers";
    pub const CLIENT_ID: &'static str = "client.id";
    pub const MESSAGE_TIMEOUT_MS: &'static str = "message.timeout.ms";
    pub const ENABLE_AUTO_COMMIT: &'static str = "enable.auto.commit";
    pub const AUTO_OFFSET_COMMIT: &'static str = "auto.offset.reset";
    pub const GROUP_ID: &'static str = "group.id";
    pub const MAX_POLL_RECORDS: &'static str = "max.poll.records";
    pub const ACKS: &'static str = "acks";
}
