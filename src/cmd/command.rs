use std::{
    fmt::Display,
    io::{Cursor, Read},
    vec,
};

use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};

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
