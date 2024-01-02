use std::io::{self, Error};

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
    pub async fn broker_info(&mut self) -> io::Result<()> {
        let buffer = BrokerCommand::new().into_bytes();
        self.connection.write_bytes(buffer).await?;

        let _response = self.connection.read_response().await?;
        Ok(())
    }
}
