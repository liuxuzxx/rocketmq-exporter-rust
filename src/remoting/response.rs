use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use crate::util::json::Tokenizer;

#[derive(Deserialize, Debug)]
pub struct BrokerInformation {
    #[serde(rename = "brokerAddrTable")]
    broker_addr_table: HashMap<String, BrokerData>,
    #[serde(rename = "clusterAddrTable")]
    cluster_addr_table: HashMap<String, HashSet<String>>,
}

#[derive(Deserialize, Debug)]
struct BrokerData {
    cluster: String,
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "brokerAddrs")]
    broker_addrs: HashMap<i64, String>,
}

impl BrokerInformation {
    pub fn parse(source: String) -> BrokerInformation {
        let json = Tokenizer::new(source).regular_json();
        let b: BrokerInformation = serde_json::from_str(&json).unwrap();
        b
    }
}

#[derive(Deserialize, Debug)]
pub struct TopicRouteInformation {
    #[serde(rename = "orderTopicConf", default)]
    order_topic_conf: String,

    #[serde(rename = "brokerDatas")]
    broker_datas: Vec<BrokerData>,

    #[serde(rename = "filterServerTable", default)]
    filter_server_table: HashMap<String, Vec<String>>,

    #[serde(rename = "queueDatas")]
    queue_datas: Vec<QueueData>,
}

impl TopicRouteInformation {
    pub fn parse(source: String) -> TopicRouteInformation {
        let json = Tokenizer::new(source).regular_json();
        serde_json::from_str(&json).unwrap()
    }
}
#[derive(Deserialize, Debug)]
struct QueueData {
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "readQueueNums")]
    read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    write_queue_nums: i32,
    perm: i32,
    #[serde(rename = "topicSysFlag")]
    topic_sys_flag: i32,
}

#[derive(Deserialize, Debug)]
pub struct Topics {
    #[serde(rename = "topicList")]
    topic_list: Vec<String>,
}

impl Topics {
    pub fn parse(source: String) -> Topics {
        serde_json::from_str(&source).unwrap()
    }

    pub fn topics(&self) -> &Vec<String> {
        &self.topic_list
    }
}
