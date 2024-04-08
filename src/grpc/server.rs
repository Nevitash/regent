use std::error;

use log::debug;
use rdkafka::error::KafkaError;
use tonic::{transport::Server, Request, Response, Status};

use crate::kafka::create_topic;

use self::kafka_service::{
    kafka_service_server::KafkaService, CreateTopicRequest, CreateTopicResponse,
};

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

        match create_topic(
            &request_inner.broker_string,
            &request_inner.group_id,
            &request_inner.client_id,
            &request_inner.topic,
        )
        .await
        {
            Ok(_) => {
                let response = kafka_service::CreateTopicResponse {
                    success: true,
                    error_message: String::new(),
                };
                Ok(Response::new(response))
            }
            Err(error) => Err(error.to_status()),
        }
    }
}

pub trait ToStatus {
    fn to_status(&self) -> Status;
}

impl ToStatus for KafkaError {
    fn to_status(&self) -> Status {
        match &self {
            KafkaError::AdminOpCreation(message) => Status::already_exists(message),
            _ => Status::unknown("Internal Server Error"),
        }
    }
}
