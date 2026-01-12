use futures::{SinkExt, StreamExt};
use yellowstone_grpc_proto::prelude::SubscribeUpdate;

pub mod client;
pub mod kafka;
pub mod router;
pub mod subscriptions;

pub struct YellowstoneWorker {
    endpoint: String,
    x_token: Option<String>,
    kafka_brokers: String,
    subscription_config: subscriptions::SubscriptionConfig,
}

impl YellowstoneWorker {
    pub fn new(
        endpoint: String,
        x_token: Option<String>,
        kafka_brokers: String,
        subscription_config: subscriptions::SubscriptionConfig,
    ) -> Self {
        Self {
            endpoint,
            x_token,
            kafka_brokers,
            subscription_config,
        }
    }

    pub async fn run(self) {
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();

        let publisher = match kafka::KafkaPublisher::new(&self.kafka_brokers) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to create Kafka producer: {}", e);
                return;
            }
        };

        if let Err(e) = publisher.check_connection() {
            eprintln!("Kafka connection failed: {}", e);
            return;
        }

        println!(
            "Connected to Kafka at {} (topic prefix: {})",
            self.kafka_brokers, self.subscription_config.topic_prefix
        );

        let mut backoff = std::time::Duration::from_secs(1);

        loop {
            println!("Yellowstone Worker started! Connecting to {}...", endpoint);

            let mut client = match client::connect(&endpoint, x_token.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect to Yellowstone gRPC: {}", e);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                    continue;
                }
            };

            println!("Connected to Yellowstone gRPC!");

            let request = subscriptions::create_subscription_request(&self.subscription_config);

            let (mut subscribe_tx, mut stream) = match client.subscribe().await {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("Failed to subscribe: {}", e);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                    continue;
                }
            };

            if let Err(e) = subscribe_tx.send(request).await {
                eprintln!("Failed to send subscription request: {}", e);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                continue;
            }

            println!("Subscribed to updates! Waiting for data...");
            backoff = std::time::Duration::from_secs(1);

            let mut should_reconnect = false;
            while let Some(message) = stream.next().await {
                match message {
                    Ok(update) => {
                        self.log_update(&publisher, update).await;
                    }
                    Err(e) => {
                        eprintln!("Stream error: {}", e);
                        should_reconnect = true;
                        break;
                    }
                }
            }

            if !should_reconnect {
                println!("Stream ended");
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
        }
    }

    async fn log_update(&self, publisher: &kafka::KafkaPublisher, update: SubscribeUpdate) {
        let payload = router::build_payload(&update);
        let payload_json = router::serialize_payload(&payload);
        let topics = self
            .subscription_config
            .topics_for_update(&payload.filters, &payload.program_ids);
        println!("{payload_json}");

        for topic in topics {
            if let Err(err) = publisher
                .send_to(&topic, &payload.event_id, &payload_json)
                .await
            {
                eprintln!("Kafka send failed: {}", err);
            }
        }
    }
}
