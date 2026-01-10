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
    kafka_topic: String,
}

impl YellowstoneWorker {
    pub fn new(
        endpoint: String,
        x_token: Option<String>,
        kafka_brokers: String,
        kafka_topic: String,
    ) -> Self {
        Self {
            endpoint,
            x_token,
            kafka_brokers,
            kafka_topic,
        }
    }

    pub async fn run(self) {
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();

        let publisher = match kafka::KafkaPublisher::new(&self.kafka_brokers, &self.kafka_topic) {
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
            "Connected to Kafka at {} (topic: {})",
            self.kafka_brokers, self.kafka_topic
        );

        println!("Yellowstone Worker started! Connecting to {}...", endpoint);

        let mut client = match client::connect(&endpoint, x_token).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to connect to Yellowstone gRPC: {}", e);
                return;
            }
        };

        println!("Connected to Yellowstone gRPC!");

        let request = subscriptions::create_subscription_request();

        let (mut subscribe_tx, mut stream) = match client.subscribe().await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("Failed to subscribe: {}", e);
                return;
            }
        };

        if let Err(e) = subscribe_tx.send(request).await {
            eprintln!("Failed to send subscription request: {}", e);
            return;
        }

        println!("Subscribed to updates! Waiting for data...");

        loop {
            match stream.next().await {
                Some(Ok(update)) => {
                    self.log_update(&publisher, update).await;
                }
                Some(Err(e)) => {
                    eprintln!("Stream error: {}", e);
                }
                None => {
                    println!("Stream ended");
                    break;
                }
            }
        }

        println!("Yellowstone Worker shutting down...");
    }

    async fn log_update(&self, publisher: &kafka::KafkaPublisher, update: SubscribeUpdate) {
        let payload = router::build_payload(&update);
        let payload_json = router::serialize_payload(&payload);
        println!("{payload_json}\n=============================");

        if let Err(err) = publisher.send(&payload.event_id, &payload_json).await {
            eprintln!("Kafka send failed: {}", err);
        }
    }
}
