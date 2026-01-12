use std::collections::HashMap;

use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
};

const DEFAULT_TOKEN_OWNERS: [&str; 2] = [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
];

#[derive(Debug, Clone)]
pub struct FilterConfig {
    pub name: String,
    pub owners: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub topic_prefix: String,
    pub filters: Vec<FilterConfig>,
    pub max_filters: usize,
}

impl SubscriptionConfig {
    pub fn from_env(topic_prefix: String) -> Self {
        let filters = std::env::var("YELLOWSTONE_FILTERS")
            .ok()
            .and_then(|value| {
                let parsed = parse_filters(&value);
                if parsed.is_empty() {
                    None
                } else {
                    Some(parsed)
                }
            })
            .unwrap_or_else(default_filters);
        let max_filters = std::env::var("YELLOWSTONE_MAX_FILTERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);

        Self {
            topic_prefix,
            filters,
            max_filters,
        }
    }

    pub fn topics_for_update(&self, filters: &[String], program_ids: &[String]) -> Vec<String> {
        let mut topics = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut matched = false;

        for filter in &self.filters {
            if filters.iter().any(|name| name == &filter.name) {
                let topic = format!("{}.{}", self.topic_prefix, filter.name);
                if seen.insert(topic.clone()) {
                    topics.push(topic);
                }
                matched = true;
            }
        }

        if !matched {
            for filter in &self.filters {
                if filter
                    .owners
                    .iter()
                    .any(|owner| program_ids.iter().any(|id| id == owner))
                {
                    let topic = format!("{}.{}", self.topic_prefix, filter.name);
                    if seen.insert(topic.clone()) {
                        topics.push(topic);
                    }
                }
            }
        }

        if topics.is_empty() {
            topics.push(format!("{}.raw", self.topic_prefix));
        }

        topics
    }
}

pub fn create_subscription_request(config: &SubscriptionConfig) -> SubscribeRequest {
    let mut transactions = HashMap::new();

    let filter_limit = if config.max_filters == 0 {
        None
    } else {
        Some(config.max_filters)
    };

    if let Some(limit) = filter_limit {
        if limit < config.filters.len() {
            let mut account_include = std::collections::HashSet::new();
            for filter in &config.filters {
                for owner in &filter.owners {
                    account_include.insert(owner.clone());
                }
            }

            transactions.insert(
                "combined".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: account_include.into_iter().collect(),
                    account_exclude: vec![],
                    account_required: vec![],
                },
            );
        } else {
            for filter in &config.filters {
                if filter.owners.is_empty() {
                    continue;
                }

                transactions.insert(
                    filter.name.clone(),
                    SubscribeRequestFilterTransactions {
                        vote: Some(false),
                        failed: None,
                        signature: None,
                        account_include: filter.owners.clone(),
                        account_exclude: vec![],
                        account_required: vec![],
                    },
                );
            }
        }
    } else {
        for filter in &config.filters {
            if filter.owners.is_empty() {
                continue;
            }

            transactions.insert(
                filter.name.clone(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: filter.owners.clone(),
                    account_exclude: vec![],
                    account_required: vec![],
                },
            );
        }
    }

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

fn default_filters() -> Vec<FilterConfig> {
    vec![FilterConfig {
        name: "token".to_string(),
        owners: DEFAULT_TOKEN_OWNERS
            .iter()
            .map(|id| (*id).to_string())
            .collect(),
    }]
}

fn parse_filters(raw: &str) -> Vec<FilterConfig> {
    let mut filters = Vec::new();

    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        let Some((name, owners_raw)) = entry.split_once('=') else {
            continue;
        };

        let owners = owners_raw
            .split(',')
            .map(|owner| owner.trim())
            .filter(|owner| !owner.is_empty())
            .map(|owner| owner.to_string())
            .collect::<Vec<_>>();

        let name = name.trim();
        if name.is_empty() || owners.is_empty() {
            continue;
        }

        filters.push(FilterConfig {
            name: name.to_string(),
            owners,
        });
    }

    filters
}
