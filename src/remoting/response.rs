use std::{
    char,
    collections::{HashMap, HashSet},
};

use serde::Deserialize;

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
        let json = parse_json(source);
        let b: BrokerInformation = serde_json::from_str(&json).unwrap();
        b
    }
}

#[derive(Deserialize, Debug)]
pub struct TopicRouteInformation {
    #[serde(rename = "orderTopicConf")]
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
        let json = parse_json(source);
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

///
/// 处理RocketMQ的非正规化JSON数据，转成正规化的JSON数据
///
pub fn parse_json(source: String) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut regular_json = String::from("");

    let mut index = 0;
    while index < chars.len() {
        let ch = chars.get(index).unwrap();
        regular_json.push(*ch);
        index = index + 1;
        if *ch == '{' || *ch == ',' {
            let key = parse_key(&mut index, &chars);
            match key {
                Ok(key) => {
                    if key.starts_with("\"") {
                        regular_json.push_str(&key);
                        regular_json.push(':');
                    } else {
                        let mut temp = String::from("");
                        temp.push('"');
                        temp.push_str(&key);
                        temp.push('"');
                        temp.push(':');
                        regular_json.push_str(&temp);
                    }
                }
                Err(data) => regular_json.push_str(&data),
            }
        }
    }
    println!("regular json:{regular_json}");
    regular_json
}

fn parse_key(i: &mut usize, chars: &Vec<char>) -> Result<String, String> {
    let mut key = String::from("");
    while *i < chars.len() {
        let ch = chars.get(*i).unwrap();
        *i = *i + 1;
        if *ch == ':' {
            return Ok(key);
        } else if *ch != '\n' && *ch != ' ' {
            key.push(*ch);
        }
    }
    return Err(String::from(key));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json() {
        let json = r#"
            {
                "brokerAddrTable":{
                    "broker-b":{
                        "brokerAddrs":{
                            0:"10.20.141.72:20911"
                        },
                        "brokerName":"broker-b",
                        "cluster":"rocketmq-cpaas"
                    },
                    "broker-a":{
                        "brokerAddrs":{
                            0:"10.20.141.73:20911"
                        },
                        "brokerName":"broker-a",
                        "cluster":"rocketmq-cpaas"
                    }
                },
                "clusterAddrTable":{
                    "rocketmq-cpaas":["broker-b","broker-a"]
                }
            }
        "#;
        let result = parse_json(json.to_string());
        println!("规范之后的结果:{result}")
    }
}
