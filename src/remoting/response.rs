use std::collections::{HashMap, HashSet};

pub struct BrokerInformation {
    broker_addr_table: HashMap<String, BrokerData>,
    cluster_addr_table: HashMap<String, HashSet<String>>,
}

struct BrokerData {
    cluster: String,
    broker_name: String,
    broker_addrs: HashMap<i64, String>,
}
