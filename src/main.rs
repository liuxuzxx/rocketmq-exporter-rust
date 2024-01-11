use remoting::client::Client;

mod cmd;
mod remoting;

#[tokio::main]
pub async fn main() {
    println!("Start rocketmq exporter...");
    //fetch_broker_info().await;
    broker_operation().await;
}

async fn fetch_broker_info() {
    println!("debug the broker information process......");
    let addr = String::from("rocketmq-cloud.cpaas-test:9876");
    let mut client = Client::connection(addr).await.unwrap();
    println!("获取Broker信息");
    client.broker_info().await.unwrap();
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
