use std::error;

use log::debug;
use rdkafka::error::KafkaError;
use tonic::{transport::Server, Request, Response, Status};

use crate::kafka::create_topic;

use self::kafka_service::{kafka_service_server::KafkaService, CreateTopicResponse, CreateTopicRequest};

pub mod kafka_service {
    tonic::include_proto!("kafka");
}

pub struct KafkaServiceImpl {}

#[tonic::async_trait]
impl KafkaService for KafkaServiceImpl {
    async fn create_topic(
        &self,
        request: tonic::Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        debug!("Got a request: {:?}", request);
        let request_inner: CreateTopicRequest = request.into_inner();
        let mut success: bool = false;
        let mut error_message: String = String::new();
        match create_topic(
            &request_inner.broker_string, 
            &request_inner.group_id, 
            &request_inner.client_id, 
            &request_inner.topic
        ).await{
            Ok => {
                success = true;
            },
            Err(error) => {
                success = false;
                error_message = error.to_string();
            }
        }
        let response = kafka_service::CreateTopicResponse {
            success,
            error_message,
        };

        Ok(Response::new(response))
    }
}

pub struct ApiError{
    message: String,
    error: Erro
}

impl From<KafkaError> for ApiError {
    fn from(value: KafkaError) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}

impl From<ApiError> for Status {
    fn from(value: ApiError) -> Self {
        Status::al
    }
}
