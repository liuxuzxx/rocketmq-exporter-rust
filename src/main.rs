use remoting::client::Client;
mod cmd;
mod remoting;

#[tokio::main]
pub async fn main() {
    println!("Start rocketmq exporter...");
    fetch_broker_info().await;
}

async fn fetch_broker_info() {
    println!("debug the broker information process......");
    let addr = String::from("rocketmq-cloud.cpaas-test:9876");
    let mut client = Client::connection(addr).await.unwrap();
    println!("获取Broker信息");
    client.broker_info().await;
}
