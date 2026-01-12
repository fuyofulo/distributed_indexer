use std::error::Error;

mod yellowstone;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();

    let yellowstone_endpoint = std::env::var("YELLOWSTONE_ENDPOINT").unwrap();
    let yellowstone_token = std::env::var("YELLOWSTONE_TOKEN").ok();
    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let kafka_topic_prefix =
        std::env::var("KAFKA_TOPIC_PREFIX").unwrap_or_else(|_| "ingest".to_string());
    let subscription_config =
        yellowstone::subscriptions::SubscriptionConfig::from_env(kafka_topic_prefix.clone());

    println!("Starting Yellowstone ingestor...");
    println!("   Endpoint: {}", yellowstone_endpoint);
    println!(
        "   Token: {}",
        if yellowstone_token.is_some() {
            "Set"
        } else {
            "Not set"
        }
    );
    println!("   Kafka brokers: {}", kafka_brokers);
    println!("   Kafka topic prefix: {}", kafka_topic_prefix);
    println!(
        "   Filters: {}",
        subscription_config
            .filters
            .iter()
            .map(|cfg| cfg.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let worker = yellowstone::YellowstoneWorker::new(
        yellowstone_endpoint,
        yellowstone_token,
        kafka_brokers,
        subscription_config,
    );
    worker.run().await;

    Ok(())
}
