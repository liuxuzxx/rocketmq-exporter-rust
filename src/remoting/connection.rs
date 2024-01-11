use std::io;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::cmd::command::RemotingCommand;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(40 * 1024),
        }
    }

    pub async fn write_bytes(&mut self, buffer: BytesMut) -> io::Result<()> {
        let result = self.stream.write(&buffer).await?;
        println!("send data size:{}", result);
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_response(&mut self) -> io::Result<()> {
        loop {
            let count = self.stream.read_buf(&mut self.buffer).await?;
            if count == 0 {
                println!(
                    "read data size:{count} from server content is:{:?}",
                    String::from_utf8_lossy(&self.buffer[..])
                );
                break;
            } else {
                println!(
                    "read data size:{count} content:{:?}",
                    String::from_utf8_lossy(&self.buffer[..])
                );
            }
        }

        let response = RemotingCommand::parse(&self.buffer);
        println!("RocketMQ response:{}", response);
        self.buffer.clear();
        Ok(())
    }
}
