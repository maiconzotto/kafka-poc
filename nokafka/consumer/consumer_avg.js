const Kafka = require('no-kafka');
var Promise = require('bluebird');

var valueSum = 0;
var count = 1

// Create an instance of the Kafka consumer
const consumer = new Kafka.SimpleConsumer({"connectionString":"127.0.0.1:9092"})


var data = function (messageSet, topic, partition) {
    return Promise.each(messageSet, function (m){
        var value = parseInt(m.message.value.toString('utf8'));
        valueSum = valueSum + value;
        count = count + 1;
        console.log("Topic: " + topic, ", Partition: " + partition, ", Offset: " + m.offset, 
            ", Message: " + valueSum/count);
        return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
};

// Subscribe to the Kafka topic
/*
return consumer.init().then(function () {
    return consumer.subscribe('price-change', data);
});
*/

return consumer.init().then(function () {
      return consumer.subscribe('price-change', data);
});