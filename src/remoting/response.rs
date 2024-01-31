use std::{
    collections::{HashMap, HashSet},
    vec,
};

use serde::{de, Deserialize, Serialize};
use serde_json::Value;

use crate::util::json::Tokenizer;

///
/// RocketMQ的信息的Master的ID，是: 0
const MASTER_KEY: i64 = 0;
#[derive(Deserialize, Debug)]
pub struct BrokerInformation {
    #[serde(rename = "brokerAddrTable")]
    broker_addr_table: HashMap<String, BrokerData>,
    #[serde(rename = "clusterAddrTable")]
    cluster_addr_table: HashMap<String, HashSet<String>>,
}

impl BrokerInformation {
    pub fn parse(source: String) -> BrokerInformation {
        let json = Tokenizer::new(source).regular_json();
        let b: BrokerInformation = serde_json::from_str(&json).unwrap();
        b
    }

    pub fn all_broker_addrs(&self) -> Vec<&String> {
        let mut addrs = vec![];
        for (_, value) in self.broker_addr_table.iter() {
            match value.master_broker_addrs() {
                Some(addr) => {
                    addrs.push(addr);
                }
                None => continue,
            }
        }
        addrs
    }
}

#[derive(Deserialize, Debug)]
struct BrokerData {
    cluster: String,
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "brokerAddrs")]
    broker_addrs: HashMap<i64, String>,
}

impl BrokerData {
    fn master_broker_addrs(&self) -> Option<&String> {
        self.broker_addrs.get(&MASTER_KEY)
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

#[derive(Debug, Serialize)]
pub struct TopicStats {
    #[serde(rename = "offsetTable")]
    offset_table: HashMap<MessageQueue, TopicOffset>,
}

impl TopicStats {
    pub fn parse(source: String) -> TopicStats {
        serde_json::from_str(&source.as_str()).unwrap()
    }
}

impl<'de> Deserialize<'de> for TopicStats {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        println!("查看读取的Value:{:?}", value);
        Ok(Self {
            offset_table: HashMap::new(),
        })
    }
}

#[derive(Debug, Hash)]
struct MessageQueue {
    broker_name: String,
    queue_id: i32,
    topic: String,
}

impl PartialEq for MessageQueue {
    fn eq(&self, other: &Self) -> bool {
        self.broker_name.eq(&other.broker_name)
            && self.queue_id.eq(&other.queue_id)
            && self.topic.eq(&other.topic)
    }
    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl Eq for MessageQueue {}

///
/// 实现单独的序列化接口
impl Serialize for MessageQueue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let json = format!("{}-{}-{}", self.broker_name, self.queue_id, self.topic);
        serializer.serialize_str(&json)
    }
}

impl<'de> Deserialize<'de> for MessageQueue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = <&str>::deserialize(deserializer)?;

        println!("查看接收到的字符串:{data}");
        Ok(Self {
            broker_name: "test".to_string(),
            queue_id: 1,
            topic: "prod_test".to_string(),
        })
    }
}

#[derive(Deserialize, Debug, Serialize)]
struct TopicOffset {
    #[serde(rename = "minOffset")]
    min_offset: i64,
    #[serde(rename = "maxOffset")]
    max_offset: i64,
    #[serde(rename = "lastUpdateTimestamp")]
    last_update_timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_complex_hashmap() {
        let mut offset_table = HashMap::new();
        let message_queue = MessageQueue {
            broker_name: "broker-a".to_string(),
            queue_id: 8,
            topic: "prod_CPAAS_CHANNEL_EVENT".to_string(),
        };
        let topic_offset = TopicOffset {
            min_offset: 90,
            max_offset: 1293,
            last_update_timestamp: 888234,
        };

        offset_table.insert(message_queue, topic_offset);

        let topic_stats = TopicStats {
            offset_table: offset_table,
        };
        let json = serde_json::to_string(&topic_stats).unwrap();
        println!("序列化之后的字符串:{json}");
    }
}
