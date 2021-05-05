const { Kafka } = require('kafkajs')

var express = require('express');
var app = express();
app.use(express.json())

const clientId = "producer"

const brokers = ["127.0.0.1:9092"]

const topic = "price-change"

const kafka = new Kafka({ clientId, brokers })
const producer = kafka.producer()


var sendMessage = async (messages) => {
    await producer.connect()
    await producer.send({
      topic: topic,
      messages: messages
      ,
    })
    await producer.disconnect()
  }

app.post('/producer', function (req, res) {

    var j = 0;
    var body = req.body;
    var messages = [];

    while(body.i > j){
        
        messages.push({
            key : body.key,
            value : j.toString()
        });
        j++;
    } 

    sendMessage(messages);
    
    res.status(201).send({
        "resultado" : body.i+ " mensagens enviadas!"
    });
});

app.listen(3000);