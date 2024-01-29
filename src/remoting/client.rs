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
    broker_connections: Option<Vec<Connection>>,
}

impl Client {
    pub async fn connection<T: ToSocketAddrs>(addr: T) -> Result<Client, Error> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);
        Ok(Client {
            connection,
            broker_connections: None,
        })
    }

    pub async fn broker_connection(&mut self) {
        let broker_info = self.broker_info().await;
        let all_broker_addrs = broker_info.all_broker_addrs();

        let mut broker_connections = vec![];
        for addr in all_broker_addrs.iter() {
            let socket = TcpStream::connect(*addr).await.unwrap();
            let connection = Connection::new(socket);
            broker_connections.push(connection);
        }
        self.broker_connections = Some(broker_connections);
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
