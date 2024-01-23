use std::io;

use futures::{SinkExt, StreamExt};
use remoting::client::Client;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::cmd::command::{RemotingCommand, RequestCode};

mod cmd;
mod remoting;

#[tokio::main]
pub async fn main() {
    println!("Start rocketmq exporter...");
    fetch_broker_info().await;
    //broker_operation().await;
}

async fn fetch_broker_info() {
    println!("debug the broker information process......");
    let addr = String::from("rocketmq-cloud.cpaas-test:9876");
    let mut client = Client::connection(addr).await.unwrap();
    println!("获取Broker信息");
    client.broker_info().await;
}

///
/// 一些Broker的操作，因为这些操作需要和Broker进行通信，所以需要Broker的地址信息
///
async fn broker_operation() {
    let addr = String::from("10.20.141.72:20911");
    println!("获取Broker的Runtime的监控信息");
    let mut client = Client::connection(addr).await.unwrap();
    client.broker_runtime_info().await.unwrap();
}

async fn frame_broker_operation() {
    let addr = String::from("10.20.141.72:20911");
    println!("采用Frame的形式获取数据......");
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    let buffer = RemotingCommand::new(RequestCode::GetBrokerRuntimeInfo).encode_no_length();
    let send_datas = buffer.freeze();
    println!("查看发送的内容:{:?}", String::from_utf8_lossy(&send_datas));
    transport.send(send_datas).await.unwrap();
    println!("Send data to broker...");
    if let Some(Ok(data)) = transport.next().await {
        println!("Get data from broker:{:?}", String::from_utf8_lossy(&data));
    }
}
