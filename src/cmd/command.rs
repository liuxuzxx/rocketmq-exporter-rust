use std::{
    collections::HashMap,
    fmt::Display,
    io::{Cursor, Read},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::codec;

///
/// RocketMQ的RemotingCommand的Request和Response的格式一致
///
#[derive(Debug)]
pub struct RemotingCommand {
    header: Header,
    body: String,
}

impl RemotingCommand {
    pub fn parse(content: &BytesMut) -> RemotingCommand {
        let mut buf = Cursor::new(&content[..]);
        let length = content.len() as i32;
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
        return RemotingCommand {
            header: Header::parse(String::from_utf8(header_data).unwrap()),
            body: String::from_utf8(body_data).unwrap(),
        };
    }

    pub fn encode_no_length(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        let header = self.header.encode();
        let length = header.len();
        buffer.put_u8(0 as u8);
        buffer.put_u8(((length >> 16) & 0xFF) as u8);
        buffer.put_u8(((length >> 8) & 0xFF) as u8);
        buffer.put_u8((length & 0xFF) as u8);
        buffer.put(Bytes::from(header.into_bytes()));

        return buffer;
    }

    pub fn new(code: RequestCode) -> RemotingCommand {
        RemotingCommand {
            header: Header::new(code),
            body: String::from(""),
        }
    }

    pub fn build<T: CustomHeader>(code: RequestCode, custom_header: Option<T>) -> RemotingCommand {
        let mut header = Header::new(code);

        if let Some(data) = custom_header {
            let encode_header = data.encode();
            header.ext_fields = encode_header;
        }
        RemotingCommand {
            header: header,
            body: String::from(""),
        }
    }

    pub fn body(&self) -> &str {
        &self.body
    }
}

impl Display for RemotingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:{}", self.header)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    code: i32,
    flag: i32,
    language: LanguageCode,
    opaque: i32,
    #[serde(rename = "serializeTypeCurrentRPC")]
    serialize_type_current_rpc: String,
    version: i32,
    #[serde(rename = "extFields", default)]
    ext_fields: HashMap<String, String>,
}

impl Header {
    pub fn new(request_code: RequestCode) -> Header {
        Header {
            code: request_code.code(),
            flag: 0,
            language: LanguageCode::RUST(String::from("RUST")),
            opaque: 1,
            serialize_type_current_rpc: String::from(""),
            version: 317,
            ext_fields: HashMap::new(),
        }
    }

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

    pub fn opaque(&self) -> i32 {
        self.opaque
    }

    pub fn serialize_type_current_rpc(&self) -> String {
        self.serialize_type_current_rpc.clone()
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn encode(&self) -> String {
        let header_json = serde_json::to_string(&self).unwrap();
        header_json
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "code:{} flag:{} version:{} serialize_type:{} language:{}",
            self.code(),
            self.flag(),
            self.version(),
            self.serialize_type_current_rpc(),
            self.language
        )
    }
}

///
/// 枚举RocketMQ的Admin需要的一些类型信息
///
#[derive(Debug)]
pub enum LanguageCode {
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
            LanguageCode::JAVA(_) => serializer.serialize_str("JAVA"),
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
            "JAVA" => Ok(LanguageCode::JAVA(String::from("JAVA"))),
            "GO" => Ok(LanguageCode::GO(String::from("GO"))),
            "RUST" => Ok(LanguageCode::RUST(String::from("RUST"))),
            _ => Ok(LanguageCode::RUST(String::from("RUST"))),
        };
        v
    }
}

impl Display for LanguageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            LanguageCode::GO(_x) => "GO",
            LanguageCode::JAVA(_x) => "JAVA",
            LanguageCode::RUST(_x) => "RUST",
        };
        write!(f, "{}", v)
    }
}

///
/// RocketMQ的RequestCode
///
pub enum RequestCode {
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

pub struct CommandCoderc {}

impl CommandCoderc {
    const MAX_SIZE: usize = 1024 * 1024 * 1024 * 8;
    const PROTOCOL_LENGTH: usize = 4;
}

impl codec::Decoder for CommandCoderc {
    type Item = RemotingCommand;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let buf_len = src.len();

        if buf_len < CommandCoderc::PROTOCOL_LENGTH {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[0..4]);
        let data_len = u32::from_be_bytes(length_bytes) as usize;

        if data_len > Self::MAX_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame data length {data_len} > {}", Self::MAX_SIZE),
            ));
        }

        let frame_len = data_len + Self::PROTOCOL_LENGTH;
        if buf_len < frame_len {
            return Ok(None);
        }

        let response = RemotingCommand::parse(&src);
        Ok(Some(response))
    }
}

impl codec::Encoder<RemotingCommand> for CommandCoderc {
    type Error = std::io::Error;
    fn encode(&mut self, item: RemotingCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let header = item.header.encode();
        let length = header.len();
        let frame_size = 4 + length;

        dst.put_i32(frame_size as i32);
        dst.put_u8(0 as u8);
        dst.put_u8(((length >> 16) & 0xFF) as u8);
        dst.put_u8(((length >> 8) & 0xFF) as u8);
        dst.put_u8((length & 0xFF) as u8);
        dst.put(Bytes::from(header.into_bytes()));
        Ok(())
    }
}

///
/// 实现请求的Header的特殊化的Trait
///
pub trait CustomHeader {
    ///
    /// 提供RocketMQ请求头部的编码trait
    fn encode(&self) -> HashMap<String, String>;
}

pub struct TopicRouteInfoRequestHeader {
    topic: String,
}

impl CustomHeader for TopicRouteInfoRequestHeader {
    fn encode(&self) -> HashMap<String, String> {
        let mut data = HashMap::new();
        data.insert(String::from("topic"), self.topic.clone());
        data
    }
}

impl TopicRouteInfoRequestHeader {
    pub fn new(topic: String) -> TopicRouteInfoRequestHeader {
        TopicRouteInfoRequestHeader { topic: topic }
    }
}
