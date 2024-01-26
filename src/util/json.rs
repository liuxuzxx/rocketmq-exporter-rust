use std::usize;

pub enum TokenType {
    BeginObject(String),
    EndObject(String),
    BeginArray(String),
    EndArray(String),
    Null(String),
    Number(String),
    Boolean(String),
    SepColon(String),
    SepComma(String),
    EndDocument,
}

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

    pub fn parse(&self) {
        let mut iter = self.source.chars().enumerate();

        self.parse_string(&mut iter);
    }

    fn parse_string<T: Iterator<Item = (usize, char)>>(&self, iter: &mut T) {
        let next = iter.next();
    }
}
