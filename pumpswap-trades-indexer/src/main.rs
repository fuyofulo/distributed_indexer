use std::error::Error;

use base64::{engine::general_purpose, Engine as _};
use futures::StreamExt;
use prost::Message as _;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use serde::Deserialize;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;

mod pumpswap;

#[derive(Debug, Deserialize)]
struct KafkaPayload {
    raw_base64: String,
}

fn build_consumer_config(brokers: &str, group_id: &str) -> Result<ClientConfig, Box<dyn Error>> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");

    let security_protocol = std::env::var("KAFKA_SECURITY_PROTOCOL").ok();
    let sasl_mechanism = std::env::var("KAFKA_SASL_MECHANISM").ok();
    let sasl_username = std::env::var("KAFKA_USERNAME").ok();
    let sasl_password = std::env::var("KAFKA_PASSWORD").ok();
    let ssl_ca_location = std::env::var("KAFKA_SSL_CA_LOCATION").ok();

    if let Some(protocol) = security_protocol.as_ref() {
        config.set("security.protocol", protocol);
    }

    if let Some(mechanism) = sasl_mechanism.as_ref() {
        config.set("sasl.mechanism", mechanism);
    }

    if security_protocol
        .as_deref()
        .unwrap_or("PLAINTEXT")
        .contains("SASL")
    {
        let username = sasl_username.as_ref().ok_or("KAFKA_USERNAME is required")?;
        let password = sasl_password.as_ref().ok_or("KAFKA_PASSWORD is required")?;
        config.set("sasl.username", username);
        config.set("sasl.password", password);
    }

    if let Some(ca_location) = ssl_ca_location.as_ref() {
        config.set("ssl.ca.location", ca_location);
    }

    Ok(config)
}

fn decode_update(payload: &KafkaPayload) -> Result<SubscribeUpdate, Box<dyn Error>> {
    let raw_bytes = general_purpose::STANDARD.decode(&payload.raw_base64)?;
    let update = SubscribeUpdate::decode(raw_bytes.as_slice())?;
    Ok(update)
}

fn payload_from_message(message: &rdkafka::message::BorrowedMessage<'_>) -> Option<KafkaPayload> {
    let payload = match message.payload_view::<str>() {
        Some(Ok(payload)) => payload,
        Some(Err(_)) => return None,
        None => return None,
    };

    serde_json::from_str(payload).ok()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let group_id =
        std::env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "indexer.pumpswap".to_string());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "ingest.pumpswap".to_string());

    println!("Starting PumpSwap indexer...");
    println!("   Kafka brokers: {}", brokers);
    println!("   Kafka group: {}", group_id);
    println!("   Kafka topic: {}", topic);

    let config = build_consumer_config(&brokers, &group_id)?;
    let consumer: StreamConsumer = config.create()?;
    consumer.subscribe(&[&topic])?;

    println!("Subscribed. Waiting for messages...");

    let mut processor = pumpswap::PumpSwapProcessor::new();
    let mut stream = consumer.stream();
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                let Some(payload) = payload_from_message(&msg) else {
                    eprintln!("Skipping non-JSON Kafka payload");
                    continue;
                };

                let update = match decode_update(&payload) {
                    Ok(update) => update,
                    Err(err) => {
                        eprintln!("Failed to decode update: {}", err);
                        continue;
                    }
                };

                processor.handle_update(update);
            }
            Err(err) => {
                eprintln!("Kafka error: {}", err);
            }
        }
    }

    Ok(())
}
