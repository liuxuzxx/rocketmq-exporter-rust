use std::io::Error;

use serde::de::value;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::cmd::broker::BrokerCommand;

use super::connection::Connection;

pub struct Client {
    connection: Connection,
}

impl Client {
    pub async fn connection<T: ToSocketAddrs>(addr: T) -> Result<Client, Error> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);
        Ok(Client { connection })
    }

    ///
    /// 发送获取broker的信息的命令
    ///
    pub async fn broker_info(&mut self) -> Result<(), Error> {
        let frame = BrokerCommand::new().into_frame();
        let result = match self.connection.write_frame(&frame).await {
            Ok(()) => println!("value failed"),
            Err(err) => panic!("error:{}", err),
        };
        println!("send request to broker over!");
        Ok(())
    }
}
