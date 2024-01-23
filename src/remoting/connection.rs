use std::io;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::cmd::command::RemotingCommand;

#[derive(Debug)]
pub struct Connection {
    stream: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: Framed::new(socket, LengthDelimitedCodec::new()),
        }
    }

    pub async fn send_request(&mut self, command: RemotingCommand) -> io::Result<()> {
        let command = command.encode_no_length();
        self.stream.send(command.freeze()).await?;

        if let Some(Ok(data)) = self.stream.next().await {
            println!("Get data from server:{:?}", data);
        }
        Ok(())
    }

    pub async fn write_bytes(&mut self, buffer: BytesMut) -> io::Result<()> {
        let result = self.stream.send(buffer.freeze()).await?;
        println!("send data size:{:?}", result);
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_response(&mut self) -> io::Result<()> {
        Ok(())
    }
}
