use std::{
    fmt::Display,
    io::{Cursor, Read},
    vec,
};

use bytes::{Buf, BytesMut};
use serde::{de::Error, Deserialize, Serialize};
use serde_json::Value;

///
/// RocketMQ的RemotingCommand的Request和Response的格式一致
///
pub struct RemotingCommand {
    header: Header,
}

impl RemotingCommand {
    pub fn parse(content: &BytesMut) -> RemotingCommand {
        let mut buf = Cursor::new(&content[..]);
        let length = buf.get_i32();
        println!("content length:{}", length);

        let origin_header_length = buf.get_i32();
        println!("origin header length:{}", origin_header_length);
        let header_length = origin_header_length & 0xFFFFFF;
        println!("header length:{}", header_length);

        let mut header_data = vec![0u8; header_length as usize];
        buf.read(&mut header_data).unwrap();
        let body_length = length - 4 - header_length;
        let mut body_data = vec![0u8; body_length as usize];
        buf.read(&mut body_data).unwrap();
        println!("parse data body:{}", String::from_utf8(body_data).unwrap());
        return RemotingCommand {
            header: Header::parse(String::from_utf8(header_data).unwrap()),
        };
    }
}

impl Display for RemotingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:{}", self.header)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Header {
    code: i32,
    flag: i32,
    language: String,
    opaque: i32,
    #[serde(rename = "serializeTypeCurrentRPC")]
    serialize_type_current_rpc: String,
    version: i32,
}

impl Header {
    pub fn parse(json_data: String) -> Header {
        let h: Header = serde_json::from_str(&json_data).unwrap();
        return h;
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn flag(&self) -> i32 {
        self.flag
    }

    pub fn language(&self) -> String {
        self.language.clone()
    }

    pub fn opaque(&self) -> i32 {
        self.opaque
    }

    pub fn serialize_type_current_rpc(&self) -> String {
        self.serialize_type_current_rpc.clone()
    }

    pub fn version(&self) -> i32 {
        self.version
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "code:{} flag:{} version:{} serialize_type:{}",
            self.code(),
            self.flag(),
            self.version(),
            self.serialize_type_current_rpc()
        )
    }
}

///
/// 枚举RocketMQ的Admin需要的一些类型信息
///
enum LanguageCode {
    JAVA(String),
    GO(String),
    RUST(String),
}

impl Serialize for LanguageCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            LanguageCode::JAVA(_) => serializer.serialize_str("Java"),
            LanguageCode::GO(_) => serializer.serialize_str("GO"),
            LanguageCode::RUST(_) => serializer.serialize_str("RUST"),
        }
    }
}

impl<'de> Deserialize<'de> for LanguageCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v: Value = Deserialize::deserialize(deserializer)?;
        let v = match v {
            Value::String(v) => v,
            _ => return Err(serde::de::Error::custom("Excepted string")),
        };

        let v: Result<LanguageCode, _> = match v.as_str() {
            "Java" => Ok(LanguageCode::JAVA(String::from("Java"))),
            "GO" => Ok(LanguageCode::GO(String::from("GO"))),
            "RUST" => Ok(LanguageCode::RUST(String::from("RUST"))),
            _ => Ok(LanguageCode::RUST(String::from("RUST"))),
        };
        v
    }
}

///
/// RocketMQ的RequestCode
///
enum RequestCode {
    SendMessage,
    PullMessage,
    QueryMessage,
    QueryBrokerOffset,
    QueryConsumerOffset,
    UpateConsumerOffset,
    UpdateAndCreateTopic,
    GetAllTopicConfig,
    GetTopicConfigList,
    GetTopicNameList,
    UpdateBrokerConfig,
    GetBrokerConfig,
    TriggerDeleteFiles,
    GetBrokerRuntingInfo,
    SearchOffsetByTimestamp,
    GetmaxOffset,
    GetMinOffset,
    GetEarliestMsgStoretime,
    ViewMessageById,
    HeartBeat,
    UndergisterClient,
    ConsumerSendMsgBack,
    EndTransaction,
    GetConsumerListByGroup,
    CheckTransactionState,
    NotifyConsumerIdsChanged,
    LockBatchMq,
    UnlockBatchMq,
    GetAllConsumerOffset,
    GetAllDelayOffset,
    CheckClientConfig,
    UpdateAndCreateAclConfig,
    DeleteAclConfig,
    GetBrokerClusterAclInfo,
    UpdateGlobalWhiteAddrsConfig,
    GetBrokerClusterAclConfig,
    PutKvConfig,
    GetKvConfig,
    DeleteKvConfig,
    RegisterBroker,
    UnregisterBroker,
    GetRouteInfoByTopic,
    GetBrokerClusterInfo,
    UpdateAndCreateSubscriptionGroup,
    GetAllSubscriptionGroupConfig,
    GetTOpicStatsInfo,
    GetConsumerConnectionList,
    GetProducerConnectionList,
    WipeWritePermOfBroker,
    GetAllTopicListFromNameserver,
    DeleteSuscriptionGroup,
    GetConsumeStats,
    SuspendConsumer,
    ResumeConsuemr,
    ResetConsumerOffsetInConsumer,
    ResetConsumerOffsetInBroker,
    AdjustConsumerThreadPool,
    WhoConsumeTheMessage,
    DeleteTopicInBroker,
    DeleteTopicInNamesrv,
    GetKvlistByNamespace,
    ResetConsumerClientOffset,
    InvokeBrokerToResetOffset,
    InvokeBrokerToGetConsuemrStatus,
    QueryTopicConsumeByWho,
    GetTopicsByCluster,
    RegisterFliterServer,
    RegisterMessageFilterClass,
    QueryConsumeTimeSpan,
    GetSystemTopicListFromNs,
    GetSystemTopicListFromBroker,
    CleanExpiredConsumeQueue,
    GetConsumerRunningInfo,
    QueryCorrectionOffset,
    ConsumeMessageDirectly,
    SendMessageV2,
    GetUnitTopicList,
    GetHasUnitSubTopicList,
    GetHashUnitSubUnunitTopicList,
    CloneGroupOffset,
    ViewBrokerStatsData,
    CleanUnusedTopic,
    GetBrokerConsumeStats,
    UpdateNamesrvConfig,
    GetNamesrvConfig,
    SendBatchMessage,
    QueryConsumeQueue,
    QueryDataVersion,
    ResumeCheckHalfMessage,
    SendReplyMessage,
    SendReplyMessageV2,
    PushReplyMessageToClient,
    AddWritePermOfBroker,
    GetAllProducerInfo,
    DeleteExpiredCommitLog,
}
