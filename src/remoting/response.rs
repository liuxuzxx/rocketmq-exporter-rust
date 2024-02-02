use std::{
    collections::{HashMap, HashSet},
    vec,
};

use serde::{Deserialize, Serialize};

use crate::util::json::{TokenType, Tokenizer};

///
/// RocketMQ的信息的Master的ID，是: 0
const MASTER_KEY: i64 = 0;
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInformation {
    broker_addr_table: HashMap<String, BrokerData>,
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
#[serde(rename_all = "camelCase")]
struct BrokerData {
    cluster: String,
    broker_name: String,
    broker_addrs: HashMap<i64, String>,
}

impl BrokerData {
    fn master_broker_addrs(&self) -> Option<&String> {
        self.broker_addrs.get(&MASTER_KEY)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteInformation {
    #[serde(default)]
    order_topic_conf: String,
    broker_datas: Vec<BrokerData>,
    #[serde(default)]
    filter_server_table: HashMap<String, Vec<String>>,
    queue_datas: Vec<QueueData>,
}

impl TopicRouteInformation {
    pub fn parse(source: String) -> TopicRouteInformation {
        let json = Tokenizer::new(source).regular_json();
        serde_json::from_str(&json).unwrap()
    }
}
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct QueueData {
    broker_name: String,
    read_queue_nums: i32,
    write_queue_nums: i32,
    perm: i32,
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

#[derive(Debug)]
pub struct TopicStats {
    offset_table: HashMap<MessageQueue, TopicOffset>,
}

impl TopicStats {
    pub fn parse(source: String) -> TopicStats {
        TopicStats {
            offset_table: Self::do_parse(source),
        }
    }

    fn do_parse(source: String) -> HashMap<MessageQueue, TopicOffset> {
        let mut tokenizer = Tokenizer::new(source);
        tokenizer.parse();
        let tokens = tokenizer.tokens();
        let offset_table = &tokens[4..tokens.len() - 3];
        let mut iter = offset_table.iter();
        let mut obj_vec = vec![];
        loop {
            match iter.next() {
                Some(token_type) => match token_type {
                    TokenType::BeginObject(c) => {
                        let mut obj_token_type = vec![];
                        obj_token_type.push(token_type.clone());
                        loop {
                            match iter.next() {
                                Some(obj_type) => match obj_type {
                                    TokenType::EndObject(oc) => {
                                        obj_token_type.push(obj_type.clone());
                                        obj_vec.push(obj_token_type);
                                        break;
                                    }
                                    _ => {
                                        obj_token_type.push(obj_type.clone());
                                    }
                                },
                                None => {
                                    break;
                                }
                            }
                        }
                    }
                    _ => {}
                },
                None => {
                    break;
                }
            }
        }

        let mut iter = obj_vec.iter().enumerate();
        let mut data_map = HashMap::new();
        loop {
            match iter.next() {
                Some((_, v)) => {
                    let json = Tokenizer::do_regular_json(v);
                    let message_queue: MessageQueue = serde_json::from_str(json.as_str()).unwrap();
                    let (_, t) = iter.next().unwrap();
                    let topic_offset: TopicOffset =
                        serde_json::from_str(Tokenizer::do_regular_json(t).as_str()).unwrap();
                    data_map.insert(message_queue, topic_offset);
                }
                None => {
                    break;
                }
            }
        }
        data_map
    }
}

#[derive(Debug, Deserialize, Hash)]
#[serde(rename_all = "camelCase")]
struct MessageQueue {
    broker_name: String,
    queue_id: i32,
    topic: String,
}

impl Eq for MessageQueue {}

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

#[derive(Deserialize, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicOffset {
    min_offset: i64,
    max_offset: i64,
    last_update_timestamp: i64,
}

///
/// Topic的消费者组的列表对象
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroups {
    group_list: Vec<String>,
}

impl ConsumerGroups {
    pub fn parse(source: String) -> ConsumerGroups {
        serde_json::from_str(&source).unwrap()
    }
}

pub struct BrokerRuntimeInfo {}

pub struct BrokerRuntimeInfoTable {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_complex_hashmap() {
        let json = r#"
          {"offsetTable":
          {
            {
                "brokerName":"broker-a",
                "queueId":0,
                "topic":"%RETRY%test_submit_68985_l4"
            }:{
                "lastUpdateTimestamp":0,
                "maxOffset":0,
                "minOffset":0}
          }
          }
        "#;

        let topic_stats = TopicStats::parse(json.to_string());
    }
}
