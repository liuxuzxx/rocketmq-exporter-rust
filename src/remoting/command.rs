use std::collections::HashMap;

use serde::{Deserialize, Serialize};

const VERSION: i32 = 317;
const BROKER_RUNTIME_INFO: i32 = 28;

//序列化的规则:
//1.其他字段使用JSON序列化
//2.body单独，不需要序列化，直接使用bytes[]来村属和设置
#[derive(Serialize, Deserialize)]
pub struct Command {
    code: i32,
    language: i32,
    version: i32,
    opaque: i32,
    flag: i32,
    remark: String,
    ext_fields: HashMap<String, String>,
    #[serde(skip)]
    body: String,
}

impl Command {
    pub fn new(code: i32, body: String) -> Command {
        Command {
            code: code,
            language: 7,
            version: VERSION,
            opaque: 1,
            flag: 1,
            remark: String::from(""),
            body: body,
            ext_fields: HashMap::new(),
        }
    }

    pub fn encode(&self) {
        let header = serde_json::to_string(&self).unwrap();
        println!("序列化的Header报文信息:{}", header);
    }
}
