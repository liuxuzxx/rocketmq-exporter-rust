use std::io;

use bytes::BytesMut;
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::frame::Frame;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }
        self.stream.flush().await
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Bulk(val) => {
                self.stream.write_all(val).await?;
            }
            Frame::Integer(val) => {
                self.stream.write_i32(*val).await?;
            }
            Frame::U8(val) => {
                self.stream.write_u8(*val).await?;
            }
            Frame::Array(_val) => unreachable!(),
        }
        Ok(())
    }
}
