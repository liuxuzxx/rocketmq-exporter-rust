use std::io::{self, Error};

use tokio::net::{TcpStream, ToSocketAddrs};

use crate::cmd::{broker::BrokerCommand, command::RemotingCommand, command::RequestCode};

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
        let command = RemotingCommand::new(RequestCode::GetBrokerClusterInfo);
        self.connection.send_request(command).await;
        Ok(())
    }

    pub async fn topic_list(&mut self) -> io::Result<()> {
        let command = RemotingCommand::new(RequestCode::GetAllTopicListFromNameserver);
        self.connection.send_request(command).await;
        Ok(())
    }

    pub async fn broker_runtime_info(&mut self) -> io::Result<()> {
        let command = RemotingCommand::new(RequestCode::GetBrokerRuntimeInfo);
        self.connection.send_request(command).await;
        Ok(())
    }
}
