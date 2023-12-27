use std::{error::Error, f32::consts::E, io, vec};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Interest},
    net::TcpStream,
};

mod remoting;

#[tokio::main]
pub async fn main() {
    remoting::broker_information_command();
    let mut stream = TcpStream::connect("tekton.cpaas.wxchina.com:30669")
        .await
        .unwrap();

    loop {
        let ready = stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await
            .unwrap();

        if ready.is_readable() {
            let mut data = vec![0; 1024];
            match stream.try_read(&mut data) {
                Ok(n) => {
                    println!("read bytes count:{}", n);
                    let response = String::from_utf8_lossy(&data).to_string();
                    println!("从服务端接收到的:{}", response);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return;
                }
            }
        }

        if ready.is_writable() {
            match stream.try_write(b"GET / HTTP/1.1\r\nHost: tekton.cpaas.wxchina.com \r\n\r\n") {
                Ok(n) => {
                    println!("Write bytes count:{}!", n);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return;
                }
            }
        }
    }
}
