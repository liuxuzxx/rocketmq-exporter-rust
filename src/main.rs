use remoting::client::Client;

mod cmd;
mod frame;
mod remoting;

#[tokio::main]
pub async fn main() {
    fetch_broker_info();
}

async fn fetch_broker_info() {
    let addr = String::from("rocketmq-cloud.cpaas-test:9876");
    let mut client = match Client::connection(addr).await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };
    println!("Establish the connection");
    let _result = match client.broker_info().await {
        Ok(()) => println!("Ok is ok"),
        Err(err) => panic!("err:{}", err),
    };
}
