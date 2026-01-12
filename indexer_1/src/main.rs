use std::error::Error;

use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

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

fn format_payload(message: &rdkafka::message::BorrowedMessage<'_>) -> String {
    match message.payload_view::<str>() {
        Some(Ok(payload)) => payload.to_string(),
        Some(Err(_)) => String::from_utf8_lossy(message.payload().unwrap_or_default()).into_owned(),
        None => "<empty payload>".to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let group_id = std::env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "indexer.token".to_string());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "ingest.token".to_string());

    println!("Starting indexer_1...");
    println!("   Kafka brokers: {}", brokers);
    println!("   Kafka group: {}", group_id);
    println!("   Kafka topic: {}", topic);

    let config = build_consumer_config(&brokers, &group_id)?;
    let consumer: StreamConsumer = config.create()?;
    consumer.subscribe(&[&topic])?;

    println!("Subscribed. Waiting for messages...");

    let mut stream = consumer.stream();
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                let payload = format_payload(&msg);
                println!("{payload}");
            }
            Err(err) => {
                eprintln!("Kafka error: {}", err);
            }
        }
    }

    Ok(())
}
