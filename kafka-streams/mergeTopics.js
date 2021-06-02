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

const stream1 = kafkaStreams.getKStream("price-change-1");
const stream2 = kafkaStreams.getKStream("price-change-2");

//merge will make sure any message that is consumed on any of the streams
//will end up being emitted in the merged stream
//checkout other operations: join (outer, left, inner), combine, zip
//for other merge options
const mergedStream = stream1.merge(stream2);

/*
mergedStream
    .filter(v => !!v); //you can use this stream as usual
*/

//await for 3 kafka consumer
//and 1 kafka producer to be ready
Promise.all([
    stream1.start(),
    stream2.start(),
    mergedStream.mapJSONConvenience()
    .mapWrapKafkaValue()
    .wrapAsKafkaValue()
    .filter(v => !!v)
    .to("price-change") //BE AWARE that .to()s on a merged stream are async
]).then(_ => {
    //consume and produce for 5 seconds
    //setTimeout(kafkaStreams.closeAll.bind(kafkaStreams), 55000);
});