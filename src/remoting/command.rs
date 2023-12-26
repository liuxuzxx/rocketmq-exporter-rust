use std::collections::HashMap;

const VERSION: i32 = 317;

pub enum LanguageCode {
    JAVA,
}

pub struct Command {
    code: i32,
    language: LanguageCode,
    version: i32,
    opaque: i32,
    flag: i32,
    remark: String,
    ext_fields: HashMap<String, String>,
    body: String,
}

impl Command {
    pub fn new(code: i32, body: String) -> Command {
        Command {
            code: code,
            language: LanguageCode::JAVA,
            version: VERSION,
            opaque: 1,
            flag: 1,
            remark: String::from(""),
            body: body,
            ext_fields: HashMap::new(),
        }
    }
}
