use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use super::command;

#[derive(Serialize, Deserialize)]
pub struct BrokerCommand {
    code: i32,
    language: String,
    version: i32,
    opaque: i32,
    flag: i32,
    remark: String,
    ext_fields: HashMap<String, String>,
}

impl BrokerCommand {
    pub fn new() -> BrokerCommand {
        BrokerCommand {
            code: command::RequestCode::GetBrokerClusterInfo.code(),
            language: String::from("GO"),
            version: 317,
            opaque: 1,
            flag: 0,
            remark: String::from(""),
            ext_fields: HashMap::new(),
        }
    }

    pub fn encode(&self) -> String {
        let header = serde_json::to_string(&self).unwrap();
        return header;
    }

    pub fn into_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        let header_encode = self.encode();
        let length = header_encode.len();
        let frame_size = 4 + length;

        buffer.put_i32(frame_size as i32);
        buffer.put_u8(0 as u8);
        buffer.put_u8(((length >> 16) & 0xFF) as u8);
        buffer.put_u8(((length >> 8) & 0xFF) as u8);
        buffer.put_u8((length & 0xFF) as u8);
        buffer.put(Bytes::from(header_encode.into_bytes()));

        return buffer;
    }
}
