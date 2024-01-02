use std::{
    io::{Cursor, Read},
    vec,
};

use bytes::{Buf, Bytes, BytesMut};

///
/// RocketMQ的RemotingCommand的Request和Response的格式一致
///
pub struct Command {}

impl Command {
    pub fn parse(content: &BytesMut) {
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
        println!(
            "parse data header:{} body:{}",
            String::from_utf8(header_data).unwrap(),
            String::from_utf8(body_data).unwrap()
        );
    }
}
