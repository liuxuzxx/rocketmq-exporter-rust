use std::error::Error;

use tokio::{io::AsyncWriteExt, net::TcpStream};

mod remoting;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect("172.16.1.133:9876").await?;
    println!("Create tcp stream to RocketMQ nameserver...");
    let result = stream.write_all(b"hello world").await;
    println!("响应的结果:{:?}", result.is_ok());

    return Ok(());
}
