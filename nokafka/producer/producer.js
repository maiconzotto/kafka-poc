const Kafka = require('no-kafka');
var express = require('express');
var app = express();
app.use(express.json())
// Create an instance of the Kafka consumer
const producer = new Kafka.Producer({"connectionString":"127.0.0.1:9092"})

app.post('/producer', function(req, res){

    var body = req.body;
    var j = 0;

    var messages = [];

    while(body.i > j){
        messages.push({
            topic: 'price-change',
            message: {
                value : j,
                key : body.key
            }
        });
        j++;
    }  

    producer.init().then(function(){
        producer.send(messages);
    });

    res.status(201).send({
        "resultado" : body.i+ " mensagens enviadas!"
    });
});

app.listen(3000);