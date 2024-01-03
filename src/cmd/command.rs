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
    UpdateConsumerOffset,
    UpdateAndCreateTopic,
    GetAllTopicConfig,
    GetTopicConfigList,
    GetTopicNameList,
    UpdateBrokerConfig,
    GetBrokerConfig,
    TriggerDeleteFiles,
    GetBrokerRuntimeInfo,
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
    GetConsumerStatusFromClient,
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

impl RequestCode {
    pub fn code(&self) -> i32 {
        match *self {
            RequestCode::SendMessage => 10,
            RequestCode::PullMessage => 11,
            RequestCode::QueryMessage => 12,
            RequestCode::QueryBrokerOffset => 13,
            RequestCode::QueryConsumerOffset => 14,
            RequestCode::UpdateConsumerOffset => 15,
            RequestCode::UpdateAndCreateTopic => 17,
            RequestCode::GetAllTopicConfig => 21,
            RequestCode::GetTopicConfigList => 22,
            RequestCode::GetTopicNameList => 23,
            RequestCode::UpdateBrokerConfig => 25,
            RequestCode::GetBrokerConfig => 26,
            RequestCode::TriggerDeleteFiles => 27,
            RequestCode::GetBrokerRuntimeInfo => 28,
            RequestCode::SearchOffsetByTimestamp => 29,
            RequestCode::GetmaxOffset => 30,
            RequestCode::GetMinOffset => 31,
            RequestCode::GetEarliestMsgStoretime => 32,
            RequestCode::ViewMessageById => 33,
            RequestCode::HeartBeat => 34,
            RequestCode::UndergisterClient => 35,
            RequestCode::ConsumerSendMsgBack => 36,
            RequestCode::EndTransaction => 37,
            RequestCode::GetConsumerListByGroup => 38,
            RequestCode::CheckTransactionState => 39,
            RequestCode::NotifyConsumerIdsChanged => 40,
            RequestCode::LockBatchMq => 41,
            RequestCode::UnlockBatchMq => 42,
            RequestCode::GetAllConsumerOffset => 43,
            RequestCode::GetAllDelayOffset => 45,
            RequestCode::CheckClientConfig => 46,
            RequestCode::UpdateAndCreateAclConfig => 50,
            RequestCode::DeleteAclConfig => 51,
            RequestCode::GetBrokerClusterAclInfo => 52,
            RequestCode::UpdateGlobalWhiteAddrsConfig => 53,
            RequestCode::GetBrokerClusterAclConfig => 54,
            RequestCode::PutKvConfig => 100,
            RequestCode::GetKvConfig => 101,
            RequestCode::DeleteKvConfig => 102,
            RequestCode::RegisterBroker => 103,
            RequestCode::UnregisterBroker => 104,
            RequestCode::GetRouteInfoByTopic => 105,
            RequestCode::GetBrokerClusterInfo => 106,
            RequestCode::UpdateAndCreateSubscriptionGroup => 200,
            RequestCode::GetAllSubscriptionGroupConfig => 201,
            RequestCode::GetTOpicStatsInfo => 202,
            RequestCode::GetConsumerConnectionList => 203,
            RequestCode::GetProducerConnectionList => 204,
            RequestCode::WipeWritePermOfBroker => 205,
            RequestCode::GetAllTopicListFromNameserver => 206,
            RequestCode::DeleteSuscriptionGroup => 207,
            RequestCode::GetConsumeStats => 208,
            RequestCode::SuspendConsumer => 209,
            RequestCode::ResumeConsuemr => 210,
            RequestCode::ResetConsumerOffsetInConsumer => 211,
            RequestCode::ResetConsumerOffsetInBroker => 212,
            RequestCode::AdjustConsumerThreadPool => 213,
            RequestCode::WhoConsumeTheMessage => 214,
            RequestCode::DeleteTopicInBroker => 215,
            RequestCode::DeleteTopicInNamesrv => 216,
            RequestCode::GetKvlistByNamespace => 219,
            RequestCode::ResetConsumerClientOffset => 220,
            RequestCode::GetConsumerStatusFromClient => 221,
            RequestCode::InvokeBrokerToResetOffset => 222,
            RequestCode::InvokeBrokerToGetConsuemrStatus => 223,
            RequestCode::QueryTopicConsumeByWho => 300,
            RequestCode::GetTopicsByCluster => 224,
            RequestCode::RegisterFliterServer => 301,
            RequestCode::RegisterMessageFilterClass => 302,
            RequestCode::QueryConsumeTimeSpan => 303,
            RequestCode::GetSystemTopicListFromNs => 304,
            RequestCode::GetSystemTopicListFromBroker => 305,
            RequestCode::CleanExpiredConsumeQueue => 306,
            RequestCode::GetConsumerRunningInfo => 307,
            RequestCode::QueryCorrectionOffset => 308,
            RequestCode::ConsumeMessageDirectly => 309,
            RequestCode::SendMessageV2 => 310,
            RequestCode::GetUnitTopicList => 311,
            RequestCode::GetHasUnitSubTopicList => 312,
            RequestCode::GetHashUnitSubUnunitTopicList => 313,
            RequestCode::CloneGroupOffset => 314,
            RequestCode::ViewBrokerStatsData => 315,
            RequestCode::CleanUnusedTopic => 316,
            RequestCode::GetBrokerConsumeStats => 317,
            RequestCode::UpdateNamesrvConfig => 318,
            RequestCode::GetNamesrvConfig => 319,
            RequestCode::SendBatchMessage => 320,
            RequestCode::QueryConsumeQueue => 321,
            RequestCode::QueryDataVersion => 322,
            RequestCode::ResumeCheckHalfMessage => 323,
            RequestCode::SendReplyMessage => 324,
            RequestCode::SendReplyMessageV2 => 325,
            RequestCode::PushReplyMessageToClient => 326,
            RequestCode::AddWritePermOfBroker => 327,
            RequestCode::GetAllProducerInfo => 328,
            RequestCode::DeleteExpiredCommitLog => 329,
        }
    }
}

impl Serialize for RequestCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for RequestCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v: Value = Deserialize::deserialize(deserializer)?;
        let v = match v {
            Value::Number(x) => x.as_i64(),
            _ => return Err(serde::de::Error::custom("Excepted Number")),
        };
        Ok(RequestCode::GetBrokerClusterInfo)
    }
}
