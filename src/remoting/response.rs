use std::{
    char,
    collections::{HashMap, HashSet},
    io::Error,
};

use serde::Deserialize;

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

///
/// 处理RocketMQ的非正规化JSON数据，转成正规化的JSON数据
///
pub fn parse_json(source: String) -> String {
    let chars: Vec<char> = source.chars().collect();

    let mut index = 0;
    while index < chars.len() {
        let ch = chars.get(index).unwrap();
        index = index + 1;
        if *ch == '{' || *ch == ',' {
            let key = parse_key(&mut index, &chars);
            match key {
                Ok(key) => {
                    println!("Found key is:{key}")
                }
                _ => println!("Not found"),
            }
        }
    }

    String::from("Debug test")
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
    return Err(String::from("Not found"));
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
        assert_eq!(String::from("Debug test"), result);
    }
}
