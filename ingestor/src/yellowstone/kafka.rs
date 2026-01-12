use std::time::Duration;

use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, util::Timeout};

pub struct KafkaPublisher {
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(brokers: &str) -> Result<Self, KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer })
    }

    pub fn check_connection(&self) -> Result<(), KafkaError> {
        self.producer
            .client()
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))?;
        Ok(())
    }

    pub async fn send_to(&self, topic: &str, key: &str, payload: &str) -> Result<(), KafkaError> {
        let record = FutureRecord::to(topic).payload(payload).key(key);

        match self
            .producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
        {
            Ok(_) => Ok(()),
            Err((err, _)) => Err(err),
        }
    }
}
