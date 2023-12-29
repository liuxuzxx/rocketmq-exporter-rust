use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::frame::Frame;

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
            code: 106,
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

    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        let encode_data = self.encode();
        let length = encode_data.len();
        let frame_size = 4 + length;

        frame.push_int32(frame_size as i32);
        frame.push_u8(0 as u8);
        frame.push_u8(((length >> 16) & 0xFF) as u8);
        frame.push_u8(((length >> 8) & 0xFF) as u8);
        frame.push_u8((length & 0xFF) as u8);
        frame.push_bulk(Bytes::from(encode_data.into_bytes()));
        frame
    }
}
