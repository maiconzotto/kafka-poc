const { Kafka } = require('kafkajs')

var express = require('express');
var app = express();
app.use(express.json())

const clientId = process.argv[2]

const brokers = ["127.0.0.1:9092"]

const topic = "price-change"

const kafka = new Kafka({ clientId, brokers })
const consumer = kafka.consumer({
    groupId: process.argv[2], retry: {
        initialRetryTime: 10000,
        retries: 3
    }
})

const run = async () => {

    await consumer.connect()

    await consumer.subscribe({ topic: topic })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log("ClienteId: " + clientId + ", Topic: " + topic, ", Partition: " + partition, ", Offset: " + message.offset,
                ", Message: " + message.value.toString('utf8'))
        },
    })
}

run().catch(e => console.error("error in producer: " + e.message, e))


app.post('/consumer3', function (req, res) {
    var body = req.body;
    consumer.seek({ topic: topic, partition: body.partition, offset: body.offset })
    res.status(201).send({
        "resultado": "Offset updated"
    });
});
app.listen(3003);