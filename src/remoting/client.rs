use std::{f32::consts::E, io::Error};

use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    cmd::{
        command::RemotingCommand,
        command::{
            CustomHeader, GetTopicStatsInfoHeader, RequestCode, TopicConsumerByWhoHeader,
            TopicRouteInfoRequestHeader,
        },
    },
    remoting::response::ConsumerGroups,
};

use super::{
    connection::Connection,
    response::{BrokerInformation, TopicRouteInformation, TopicStats, Topics},
};

pub struct Client {
    connection: Connection,
    broker_connections: Vec<Connection>,
}

impl Client {
    pub async fn connection<T: ToSocketAddrs>(addr: T) -> Result<Client, Error> {
        let socket = TcpStream::connect(addr).await?;

        let mut connection = Connection::new(socket);

        let broker_connection = Client::broker_connection(&mut connection).await;

        Ok(Client {
            connection,
            broker_connections: broker_connection,
        })
    }

    pub async fn broker_connection(namesrv_connection: &mut Connection) -> Vec<Connection> {
        let command = RemotingCommand::new(RequestCode::GetBrokerClusterInfo);
        let broker_info = namesrv_connection.send_request(command).await.unwrap();
        let broker_info = BrokerInformation::parse(broker_info.body().to_string());
        let all_broker_addrs = broker_info.all_broker_addrs();

        let mut broker_connections = vec![];
        for addr in all_broker_addrs.iter() {
            let socket = TcpStream::connect(*addr).await.unwrap();
            let connection = Connection::new(socket);
            broker_connections.push(connection);
        }
        broker_connections
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
    pub async fn topic_route(&mut self, topic: String) -> Option<TopicRouteInformation> {
        let custom_header = TopicRouteInfoRequestHeader::new(topic);
        let custom_header = Some(custom_header);
        let command = RemotingCommand::build(RequestCode::GetRouteInfoByTopic, custom_header);
        let data = self.connection.send_request(command).await.unwrap();
        if data.is_success() {
            Some(TopicRouteInformation::parse(data.body().to_string()))
        } else {
            None
        }
    }

    ///
    /// 获取Topic的stats统计信息
    pub async fn topic_stats(&mut self, topic: String) -> Option<TopicStats> {
        let custom_header = Some(GetTopicStatsInfoHeader::new(topic));
        let command = RemotingCommand::build(RequestCode::GetTOpicStatsInfo, custom_header);
        let conn = self.broker_connections.get_mut(0).unwrap();
        let response = conn.send_request(command).await.unwrap();
        if response.is_success() {
            Some(TopicStats::parse(response.body().to_string()))
        } else {
            None
        }
    }

    ///
    /// 获取Topic被哪些消费者消费了
    pub async fn query_topic_consume_by_who(&mut self, topic: String) -> Option<ConsumerGroups> {
        let custom_header = Some(TopicConsumerByWhoHeader::new(topic));
        let command = RemotingCommand::build(RequestCode::QueryTopicConsumeByWho, custom_header);
        let conn = self.broker_connections.get_mut(0).unwrap();
        let response = conn.send_request(command).await.unwrap();
        if response.is_success() {
            Some(ConsumerGroups::parse(response.body().to_string()))
        } else {
            None
        }
    }

    pub async fn query_broker_runtime_info(&mut self) {
        let command = RemotingCommand::new(RequestCode::GetBrokerRuntimeInfo);
        let conn = self.broker_connections.get_mut(0).unwrap();
        let response = conn.send_request(command).await.unwrap();
    }
}
