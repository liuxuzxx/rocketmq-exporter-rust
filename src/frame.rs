use bytes::Bytes;

#[derive(Clone, Debug)]
pub enum Frame {
    U8(u8),
    Integer(i32),
    Bulk(Bytes),
    Array(Vec<Frame>),
}

impl Frame {
    pub fn array() -> Frame {
        Frame::Array(vec![])
    }

    pub fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => vec.push(Frame::Bulk(bytes)),
            _ => panic!("not an array frame"),
        }
    }

    pub fn push_int32(&mut self, value: i32) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub fn push_u8(&mut self, value: u8) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::U8(value));
            }
            _ => panic!("not an array value"),
        }
    }
}
