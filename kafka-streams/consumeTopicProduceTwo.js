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
const stream$ = kafkaStreams.getKStream("price-change");

kafkaStreams.on("error", (error) => {
    console.log("Error occured:", error.message);
});

const [one$, two$] = stream$
    .branch([() => true, () => true]);

const producerPromiseOne = one$
    .mapJSONConvenience()
    .mapWrapKafkaValue()
    .tap((msg) => console.log("one", msg))
    .to("price-change-1", null, "buffer");

const producerPromiseTwo = two$
    .mapJSONConvenience()
    .mapWrapKafkaValue()
    .tap((msg) => console.log("two", msg))
    .wrapAsKafkaValue()
    .to("price-change-2", null, "buffer");

Promise.all([
    producerPromiseOne,
    producerPromiseTwo,
    stream$.start(),
]).then(() => {
    console.log("Stream started, as kafka consumer and producers are ready.");
}, (error) => {
    console.log("Streaming operation failed to start: ", error);
});
