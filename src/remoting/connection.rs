use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
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

    pub async fn send_request(&mut self, command: RemotingCommand) -> Result<BytesMut, String> {
        let command = command.encode_no_length();
        let _result = self.stream.send(command.freeze()).await;

        if let Some(Ok(data)) = self.stream.next().await {
            println!("Get data from server:{:?}", data);
            return Ok(data);
        }
        return Err(String::from("Parse frame error!"));
    }
}
