const {KafkaStreams} = require("kafka-streams");

const config = {
    kafkaHost: "localhost:9092",
    groupId: "kafka-streams-group",
    clientName: "kafka-streams-consume-topic",
    workerPerPartition: 1,
    options: {
        sessionTimeout: 8000,
        protocol: ["roundrobin"],
        fromOffset: "earliest", //latest
        fetchMaxBytes: 1024 * 100,
        fetchMinBytes: 1,
        fetchMaxWaitMs: 10,
        heartbeatInterval: 250,
        retryMinTimeout: 250,
        autoCommit: true,
        autoCommitIntervalMs: 1000,
        requireAcks: 0,
        //ackTimeoutMs: 100,
        //partitionerType: 3
    }
};

const kafkaStreams = new KafkaStreams(config);
const stream = kafkaStreams.getKStream("price-change");

stream.forEach((message) => {
    console.log("key: ", message.key ? message.key.toString("utf8") : null);
    console.log("value: ", message.value ? message.value.toString("utf8") : null);
    console.log("partition: ", message.partition);
    console.log("size: ", message.size);
    console.log("offset: ", message.offset);
    console.log("timestamp: ", message.timestamp);
    console.log("topic: ", message.topic);
});

stream.start().then(() => {
    console.log("stream started, as kafka consumer is ready.");
}, error => {
    console.log("streamed failed to start: " + error);
});
