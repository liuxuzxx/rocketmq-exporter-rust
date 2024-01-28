use std::fmt::Display;

use serde_json::Number;

#[derive(Debug)]
pub enum TokenType {
    BeginObject(char),
    EndObject(char),
    BeginArray(char),
    EndArray(char),
    Null(String),
    Number(String),
    Boolean(String),
    SepColon(char),
    SepComma(char),
    StringValue(String),
    EndDocument,
}

#[derive(Debug)]
pub struct Tokenizer {
    source: String,
    tokens: Vec<TokenType>,
}

///
/// 参考这个文章实现JSON的解析，使用词法分析
impl Tokenizer {
    pub fn new(source: String) -> Tokenizer {
        Tokenizer {
            source: source,
            tokens: vec![],
        }
    }

    fn parse(&mut self) {
        let chars = self.source.chars();
        let mut iter = chars.enumerate();

        loop {
            match iter.next() {
                Some((i, c)) => match c {
                    '{' => self.tokens.push(TokenType::BeginObject('{')),
                    '}' => self.tokens.push(TokenType::EndObject('}')),
                    '[' => self.tokens.push(TokenType::BeginArray('[')),
                    ']' => self.tokens.push(TokenType::EndArray(']')),
                    ',' => self.tokens.push(TokenType::SepComma(',')),
                    ':' => self.tokens.push(TokenType::SepColon(':')),
                    '0'..='9' | '-' => {
                        let (first, second) = self.parse_number(c, &mut iter);
                        self.tokens.push(first);
                        self.tokens.push(second);
                    }
                    '"' => {
                        let token = self.parse_string(&mut iter);
                        self.tokens.push(token);
                    }
                    _ => continue,
                },
                None => {
                    self.tokens.push(TokenType::EndDocument);
                    break;
                }
            }
        }
    }

    fn parse_string<T: Iterator<Item = (usize, char)>>(&self, iter: &mut T) -> TokenType {
        let mut value = String::from("");
        loop {
            match iter.next() {
                Some((i, c)) => match c {
                    '"' => return TokenType::StringValue(value),
                    _ => value.push(c),
                },
                None => {
                    return TokenType::StringValue(value);
                }
            }
        }
    }

    fn parse_number<T: Iterator<Item = (usize, char)>>(
        &self,
        first: char,
        iter: &mut T,
    ) -> (TokenType, TokenType) {
        let mut value = String::from("");
        value.push(first);
        loop {
            match iter.next() {
                Some((i, c)) => match c {
                    '0'..='9' => value.push(c),
                    ':' => return (TokenType::Number(value), TokenType::SepColon(c)),
                    ',' => return (TokenType::Number(value), TokenType::SepComma(c)),
                    '}' => return (TokenType::Number(value), TokenType::EndObject(c)),
                    ']' => return (TokenType::Number(value), TokenType::EndArray(c)),
                    _ => return (TokenType::Number(value), TokenType::EndDocument),
                },
                None => {
                    return (TokenType::Number(value), TokenType::EndDocument);
                }
            }
        }
    }

    ///
    /// 正规化JSON，采用解析JSON的方案来处理
    pub fn regular_json(&mut self) -> String {
        self.parse();
        let mut json = String::from("");
        let mut iter = self.tokens.iter();
        loop {
            match iter.next() {
                Some(t) => match t {
                    TokenType::BeginObject(c)
                    | TokenType::EndObject(c)
                    | TokenType::BeginArray(c)
                    | TokenType::EndArray(c) => json.push(*c),
                    TokenType::Null(s) | TokenType::Boolean(s) => json.push_str(s.as_str()),
                    TokenType::SepColon(c) | TokenType::SepComma(c) => json.push(*c),
                    TokenType::Number(s) => match iter.next() {
                        Some(next) => match next {
                            TokenType::SepColon(c) => {
                                let mut temp = String::from("\"");
                                temp.push_str(s.as_str());
                                temp.push('"');
                                json.push_str(&temp.as_str());
                                json.push(*c);
                            }
                            TokenType::EndObject(c)
                            | TokenType::EndArray(c)
                            | TokenType::SepComma(c) => {
                                json.push_str(s.as_str());
                                json.push(*c);
                            }
                            _ => {
                                return json;
                            }
                        },
                        None => {
                            return json;
                        }
                    },
                    TokenType::StringValue(s) => match iter.next() {
                        Some(next) => match next {
                            TokenType::SepColon(c) => {
                                let mut temp = String::from("\"");
                                temp.push_str(s.as_str());
                                temp.push('"');
                                json.push_str(&temp.as_str());
                                json.push(*c);
                            }
                            TokenType::EndObject(c)
                            | TokenType::EndArray(c)
                            | TokenType::SepComma(c) => {
                                json.push('"');
                                json.push_str(s.as_str());
                                json.push('"');
                                json.push(*c);
                            }
                            _ => {
                                return json;
                            }
                        },
                        None => {
                            return json;
                        }
                    },
                    TokenType::EndDocument => {
                        return json;
                    }
                },
                None => {
                    return json;
                }
            }
        }
        json
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_token() {
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

        let mut tokenizer = Tokenizer::new(json.to_string());
        let result = tokenizer.regular_json();
        println!("打印Tokenizer:{:?} 规范后的JSON:{}", tokenizer, result);
    }
}
