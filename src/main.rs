use remoting::client::Client;

mod cmd;
mod remoting;
mod util;

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
        if let Some(response) = route_reponse {
            println!("Topic:{topic},response:{response:?}");
        }
    }

    for topic in topics {
        let response = client.topic_stats(topic.clone()).await;
        if let Some(data) = response {
            println!("Topic:{topic} stats:{data:?}");
        }
    }

    for topic in topics {
        let result = client.query_topic_consume_by_who(topic.clone()).await;
        if let Some(data) = result {
            println!("Topic:{topic} consumer-groups:{data:?}");
        }
    }

    client.query_broker_runtime_info().await;
}
