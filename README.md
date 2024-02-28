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
```json
{
  "table": {
    "msgPutTotalTodayNow": "202317086",
    "scheduleMessageOffset_2": "3302481,3302481",
    "scheduleMessageOffset_3": "4368076,4368076",
    "scheduleMessageOffset_8": "1663953,1663953",
    "sendThreadPoolQueueHeadWaitTimeMills": "0",
    "putMessageDistributeTime": "[<=0ms]:0 [0~10ms]:0 [10~50ms]:0 [50~100ms]:0 [100~200ms]:0 [200~500ms]:0 [500ms~1s]:0 [1~2s]:0 [2~3s]:0 [3~4s]:0 [4~5s]:0 [5~10s]:0 [10s~]:0 ",
    "scheduleMessageOffset_9": "1161283,1161283",
    "scheduleMessageOffset_4": "4356130,4356130",
    "scheduleMessageOffset_5": "1887886,1887886",
    "queryThreadPoolQueueHeadWaitTimeMills": "0",
    "scheduleMessageOffset_6": "4341108,4341108",
    "scheduleMessageOffset_7": "4339088,4339088",
    "remainHowManyDataToFlush": "0 B",
    "msgGetTotalTodayNow": "203198287",
    "queryThreadPoolQueueSize": "0",
    "bootTimestamp": "1704677028015",
    "msgPutTotalYesterdayMorning": "202316882",
    "msgGetTotalYesterdayMorning": "203198185",
    "pullThreadPoolQueueSize": "0",
    "commitLogMinOffset": "169651208192",
    "pullThreadPoolQueueHeadWaitTimeMills": "0",
    "runtime": "[ 25 days, 8 hours, 53 minutes, 19 seconds ]",
    "dispatchMaxBuffer": "0",
    "brokerVersion": "397",
    "putTps": "0.0 0.0 0.0",
    "getMissTps": "98.79012098790122 123.2876712328767 121.89155048345326",
    "getTransferedTps": "0.0 0.0 0.0",
    "EndTransactionQueueSize": "0",
    "getTotalTps": "98.79012098790122 123.2876712328767 121.89155048345326",
    "scheduleMessageOffset_11": "1074572,1074572",
    "scheduleMessageOffset_12": "974605,974605",
    "scheduleMessageOffset_13": "781935,781935",
    "consumeQueueDiskRatio": "0.08",
    "scheduleMessageOffset_14": "738865,738865",
    "scheduleMessageOffset_10": "1095418,1095418",
    "pageCacheLockTimeMills": "0",
    "commitLogDiskRatio": "0.08",
    "getFoundTps": "0.0 0.0 0.0",
    "scheduleMessageOffset_15": "738864,738864",
    "scheduleMessageOffset_16": "738861,738861",
    "scheduleMessageOffset_17": "502813,502813",
    "scheduleMessageOffset_18": "107,107",
    "EndTransactionThreadPoolQueueCapacity": "100000",
    "commitLogDiskRatio_/home/soft/rocketmq-4.9.2_0905/store/commitlog": "0.08",
    "commitLogMaxOffset": "169891684804",
    "getMessageEntireTimeMax": "26",
    "msgPutTotalTodayMorning": "202317086",
    "putMessageTimesTotal": "202317086",
    "msgGetTotalTodayMorning": "203198287",
    "brokerVersionDesc": "V4_9_2",
    "sendThreadPoolQueueSize": "0",
    "startAcceptSendRequestTimeStamp": "0",
    "putMessageEntireTimeMax": "37",
    "earliestMessageTimeStamp": "1706162317012",
    "commitLogDirCapacity": "Total : 299.3 GiB, Free : 276.1 GiB.",
    "remainTransientStoreBufferNumbs": "2147483647",
    "queryThreadPoolQueueCapacity": "20000",
    "putMessageAverageSize": "786.4522811632429",
    "dispatchBehindBytes": "0",
    "putMessageSizeTotal": "159112733803",
    "sendThreadPoolQueueCapacity": "10000",
    "pullThreadPoolQueueCapacity": "100000"
  }
}
```