use std::{
    collections::{HashMap, HashSet},
    vec,
};

use serde::{Deserialize, Deserializer, Serialize};

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

#[derive(Debug, Deserialize)]
pub struct BrokerRuntimeInfo {
    table: BrokerRuntimeInfoTable,
}

impl BrokerRuntimeInfo {
    pub fn from(source: String) -> BrokerRuntimeInfo {
        serde_json::from_str(&source).unwrap()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerRuntimeInfoTable {
    #[serde(rename = "msgPutTotalTodayNow")]
    msg_put_total_today_now: String,

    #[serde(rename = "scheduleMessageOffset_2")]
    schedule_message_offset_2: String,

    #[serde(rename = "scheduleMessageOffset_3")]
    schedule_message_offset_3: String,

    #[serde(rename = "sendThreadPoolQueueHeadWaitTimeMills")]
    send_thread_pool_queue_head_wait_time_mills: String,

    #[serde(rename = "putMessageDistributeTime")]
    put_message_distribute_time: String,

    #[serde(rename = "scheduleMessageOffset_9")]
    schedule_message_offset_9: String,

    #[serde(rename = "scheduleMessageOffset_4")]
    schedule_message_offset_4: String,

    #[serde(rename = "scheduleMessageOffset_5")]
    schedule_message_offset_5: String,

    #[serde(rename = "queryThreadPoolQueueHeadWaitTimeMills")]
    query_thread_pool_queue_head_wait_time_mills: String,

    #[serde(rename = "scheduleMessageOffset_7")]
    schedule_message_offset_7: String,

    #[serde(rename = "scheduleMessageOffset_6")]
    schedule_message_offset_6: String,

    #[serde(rename = "remainHowManyDataToFlush")]
    remain_how_many_data_to_flush: String,

    #[serde(rename = "msgGetTotalTodayNow")]
    msg_get_total_today_now: String,

    #[serde(rename = "queryThreadPoolQueueSize")]
    query_thread_pool_queue_size: String,

    #[serde(rename = "bootTimestamp")]
    boot_timestamp: String,

    #[serde(rename = "msgPutTotalYesterdayMorning")]
    msg_put_total_yesterday_morning: String,

    #[serde(rename = "msgGetTotalYesterdayMorning")]
    msg_get_total_yesterday_morning: String,

    #[serde(rename = "pullThreadPoolQueueSize")]
    pull_thread_pool_queue_size: String,

    #[serde(rename = "commitLogMinOffset")]
    commit_log_min_offset: String,

    #[serde(rename = "pullThreadPoolQueueHeadWaitTimeMills")]
    pull_thread_pool_queue_head_wait_time_mills: String,

    runtime: String,

    #[serde(rename = "dispatchMaxBuffer")]
    dispatch_max_buffer: String,

    #[serde(rename = "brokerVersion")]
    broker_version: String,

    #[serde(rename = "putTps")]
    put_tps: String,

    #[serde(rename = "getMissTps")]
    get_miss_tps: String,

    #[serde(rename = "getTransferedTps")]
    get_transfered_tps: String,

    #[serde(rename = "EndTransactionQueueSize")]
    end_transaction_queue_size: String,

    #[serde(rename = "getTotalTps")]
    get_total_tps: String,

    #[serde(rename = "scheduleMessageOffset_11")]
    schedule_message_offset_11: String,

    #[serde(rename = "scheduleMessageOffset_12")]
    schedule_message_offset_12: String,

    #[serde(rename = "scheduleMessageOffset_13")]
    schedule_message_offset_13: String,

    #[serde(rename = "consumeQueueDiskRatio")]
    consume_queue_disk_ratio: String,

    #[serde(rename = "getFoundTps")]
    get_found_tps: String,

    #[serde(rename = "scheduleMessageOffset_16")]
    schedule_message_offset_16: String,

    #[serde(rename = "scheduleMessageOffset_17")]
    schedule_message_offset_17: String,

    #[serde(rename = "scheduleMessageOffset_18")]
    schedule_message_offset_18: String,

    #[serde(rename = "EndTransactionThreadPoolQueueCapacity")]
    end_transaction_thread_pool_queue_capacity: String,

    #[serde(rename = "commitLogMaxOffset")]
    commit_log_max_offset: String,

    #[serde(rename = "getMessageEntireTimeMax")]
    get_message_entire_time_max: String,

    #[serde(rename = "msgPutTotalTodayMorning")]
    msg_put_total_today_morning: String,

    #[serde(rename = "putMessageTimesTotal")]
    put_message_times_total: String,

    #[serde(rename = "msgGetTotalTodayMorning")]
    msg_get_total_today_morning: String,

    #[serde(rename = "brokerVersionDesc")]
    broker_version_desc: String,

    #[serde(rename = "sendThreadPoolQueueSize")]
    send_thread_pool_queue_size: String,

    #[serde(rename = "startAcceptSendRequestTimeStamp")]
    start_accept_send_request_time_stamp: String,

    #[serde(rename = "putMessageEntireTimeMax")]
    put_message_entire_time_max: String,

    #[serde(rename = "earliestMessageTimeStamp")]
    earliest_message_time_stamp: String,

    #[serde(rename = "commitLogDirCapacity")]
    commit_log_dir_capacity: String,

    #[serde(rename = "remainTransientStoreBufferNumbs")]
    remain_transient_store_buffer_numbs: String,

    #[serde(rename = "queryThreadPoolQueueCapacity")]
    query_thread_pool_queue_capacity: String,

    #[serde(rename = "putMessageAverageSize")]
    put_message_average_size: String,

    #[serde(rename = "dispatchBehindBytes")]
    dispatch_behind_bytes: String,

    #[serde(rename = "putMessageSizeTotal")]
    put_messgae_size_total: String,

    #[serde(rename = "sendThreadPoolQueueCapacity")]
    send_thread_pool_queue_capacity: String,

    #[serde(rename = "pullThreadPoolQueueCapacity")]
    pull_thread_pool_queue_capacity: String,
}

pub fn split_two_to_i64(source: String) -> (i64, i64) {
    let elements: Vec<&str> = source.split(',').collect();

    if elements.len() != 2 {
        return (-1, -1);
    }
    let first = elements.get(0).unwrap().parse::<i64>().unwrap();
    let second = elements.get(1).unwrap().parse::<i64>().unwrap();
    (first, second)
}

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

    #[test]
    fn test_deserialize_broker_runtime_stats_information() {
        let json = r#"
        {
  "table": {
    "msgPutTotalTodayNow": "202317086",
    "scheduleMessageOffset_2": "330248,13302481",
    "scheduleMessageOffset_3": "4368076,4368076",
    "scheduleMessageOffset_8": "1663953,1663953",
    "sendThreadPoolQueueHeadWaitTimeMills": "0",
    "putMessageDistributeTime": "[<=0ms]:0 [0~10ms]:0 [10~50ms]:0 [50~100ms]:0 [100~200ms]:0 [200~500ms]:0 [500ms~1s]:0 [1~2s]:0 [2~3s]:0 [3~4s]:0 [4~5s]:0 [5~10s]:0 [10s~]:0 ",
    "scheduleMessageOffset_9": "1161283,1161283",
    "scheduleMessageOffset_4": "4356130,4356130",
    "scheduleMessageOffset_5": "1887886,1887886",
    "queryThreadPoolQueueHeadWaitTimeMills": "0",
    "scheduleMessageOffset_6": "4341108,4341108",
    "scheduleMessageOffset_7": "4339088,4339088",
    "remainHowManyDataToFlush": "0 B",
    "msgGetTotalTodayNow": "203198287",
    "queryThreadPoolQueueSize": "0",
    "bootTimestamp": "1704677028015",
    "msgPutTotalYesterdayMorning": "202316882",
    "msgGetTotalYesterdayMorning": "203198185",
    "pullThreadPoolQueueSize": "0",
    "commitLogMinOffset": "169651208192",
    "pullThreadPoolQueueHeadWaitTimeMills": "0",
    "runtime": "[ 25 days, 8 hours, 53 minutes, 19 seconds ]",
    "dispatchMaxBuffer": "0",
    "brokerVersion": "397",
    "putTps": "0.0 0.0 0.0",
    "getMissTps": "98.79012098790122 123.2876712328767 121.89155048345326",
    "getTransferedTps": "0.0 0.0 0.0",
    "EndTransactionQueueSize": "0",
    "getTotalTps": "98.79012098790122 123.2876712328767 121.89155048345326",
    "scheduleMessageOffset_11": "1074572,1074572",
    "scheduleMessageOffset_12": "974605,974605",
    "scheduleMessageOffset_13": "781935,781935",
    "consumeQueueDiskRatio": "0.08",
    "scheduleMessageOffset_14": "738865,738865",
    "scheduleMessageOffset_10": "1095418,1095418",
    "pageCacheLockTimeMills": "0",
    "commitLogDiskRatio": "0.08",
    "getFoundTps": "0.0 0.0 0.0",
    "scheduleMessageOffset_15": "738864,738864",
    "scheduleMessageOffset_16": "738861,738861",
    "scheduleMessageOffset_17": "502813,502813",
    "scheduleMessageOffset_18": "107,107",
    "EndTransactionThreadPoolQueueCapacity": "100000",
    "commitLogDiskRatio_/home/soft/rocketmq-4.9.2_0905/store/commitlog": "0.08",
    "commitLogMaxOffset": "169891684804",
    "getMessageEntireTimeMax": "26",
    "msgPutTotalTodayMorning": "202317086",
    "putMessageTimesTotal": "202317086",
    "msgGetTotalTodayMorning": "203198287",
    "brokerVersionDesc": "V4_9_2",
    "sendThreadPoolQueueSize": "0",
    "startAcceptSendRequestTimeStamp": "0",
    "putMessageEntireTimeMax": "37",
    "earliestMessageTimeStamp": "1706162317012",
    "commitLogDirCapacity": "Total : 299.3 GiB, Free : 276.1 GiB.",
    "remainTransientStoreBufferNumbs": "2147483647",
    "queryThreadPoolQueueCapacity": "20000",
    "putMessageAverageSize": "786.4522811632429",
    "dispatchBehindBytes": "0",
    "putMessageSizeTotal": "159112733803",
    "sendThreadPoolQueueCapacity": "10000",
    "pullThreadPoolQueueCapacity": "100000"
  }
}
        "#;

        BrokerRuntimeInfo::from(json.to_string());
    }
}
