# 获取的Broker Information信息
```bash
获取Broker信息
Get data from server:b"\0\0\0_{\"code\":0,\"flag\":1,\"language\":\"JAVA\",\"opaque\":1,\"serializeTypeCurrentRPC\":\"JSON\",\"version\":397}{\"brokerAddrTable\":{\"broker-b\":{\"brokerAddrs\":{0:\"10.20.141.72:20911\"},\"brokerName\":\"broker-b\",\"cluster\":\"rocketmq-cpaas\"},\"broker-a\":{\"brokerAddrs\":{0:\"10.20.141.73:20911\"},\"brokerName\":\"broker-a\",\"cluster\":\"rocketmq-cpaas\"}},\"clusterAddrTable\":{\"rocketmq-cpaas\":[\"broker-b\",\"broker-a\"]}}"
content length:388
origin header length:95
header length:95
parse data body:{"brokerAddrTable":{"broker-b":{"brokerAddrs":{0:"10.20.141.72:20911"},"brokerName":"broker-b","cluster":"rocketmq-cpaas"},"broker-a":{"brokerAddrs":{0:"10.20.141.73:20911"},"brokerName":"broker-a","cluster":"rocketmq-cpaas"}},"clusterAddrTable":{"rocketmq-cpaas":["broker-b","broker-a"]}}
Parse server data:RemotingCommand { header: Header { code: 0, flag: 1, language: JAVA("JAVA"), opaque: 1, serialize_type_current_rpc: "JSON", version: 397 } }
```