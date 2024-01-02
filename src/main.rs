use remoting::client::Client;

mod cmd;
mod frame;
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
    println!("create the connection to server...");
    client.broker_info().await.unwrap();
    println!("receive response from server");
}
