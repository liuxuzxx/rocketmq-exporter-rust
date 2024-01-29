use std::io::Error;

use tokio::net::{TcpStream, ToSocketAddrs};

use crate::cmd::{
    command::RemotingCommand,
    command::{RequestCode, TopicRouteInfoRequestHeader},
};

use super::{
    connection::Connection,
    response::{BrokerInformation, TopicRouteInformation, Topics},
};

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
    pub async fn broker_info(&mut self) -> BrokerInformation {
        let command = RemotingCommand::new(RequestCode::GetBrokerClusterInfo);
        let data = self.connection.send_request(command).await.unwrap();
        let body = data.body();
        let b = BrokerInformation::parse(body.to_string());
        b
    }

    ///
    /// 从Nameserver这个地址获取到Topic信息列表
    ///
    pub async fn topic_list(&mut self) -> Topics {
        let command = RemotingCommand::new(RequestCode::GetAllTopicListFromNameserver);
        let data = self.connection.send_request(command).await.unwrap();
        Topics::parse(data.body().to_string())
    }

    ///
    /// 获取Topic的Route信息
    ///
    pub async fn topic_route(&mut self, topic: String) -> TopicRouteInformation {
        let custom_header = TopicRouteInfoRequestHeader::new(topic);
        let custom_header = Some(custom_header);
        let command = RemotingCommand::build(RequestCode::GetRouteInfoByTopic, custom_header);
        let data = self.connection.send_request(command).await.unwrap();
        TopicRouteInformation::parse(data.body().to_string())
    }
}
