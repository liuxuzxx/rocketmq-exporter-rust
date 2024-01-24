use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct BrokerInformation {
    broker_addr_table: HashMap<String, BrokerData>,
    cluster_addr_table: HashMap<String, HashSet<String>>,
}

#[derive(Deserialize)]
struct BrokerData {
    cluster: String,
    broker_name: String,
    broker_addrs: HashMap<i64, String>,
}
