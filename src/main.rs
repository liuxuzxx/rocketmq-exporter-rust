use remoting::client::Client;
mod cmd;
mod remoting;

#[tokio::main]
pub async fn main() {
    println!("Start rocketmq exporter...");
    rocketmq_metrics().await;
}

async fn rocketmq_metrics() {
    let addr = String::from("rocketmq-cloud.cpaas-test:9876");
    let mut client = Client::connection(addr).await.unwrap();
    let response = client.broker_info().await;
    println!("broker information:{:?}", response);

    let response = client.topic_list().await;
    println!("Topic List:{:?}", response);

    let topics = response.topics();
    for topic in topics {
        let route_reponse = client.topic_route(topic.clone()).await;
        println!("Topic:{} route:{:?}", topic, route_reponse);
    }
}
