const Kafka = require('no-kafka');
var Promise = require('bluebird');
var express = require('express');

var partition = process.argv[3];
var offset = process.argv[4];

const consumer = new Kafka.SimpleConsumer({"connectionString":"127.0.0.1:9092", clientId: process.argv[2]})

var dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, function (m){
        console.log("ClientId: " + consumer.options.clientId, ", Topic: " + topic, ", Partition: " + partition, ", Offset: " + m.offset, 
            ", Message: " + m.message.value.toString('utf8'));
        return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
};

return consumer.init().then(function () {

    if(offset != null && partition != null)
      return consumer.subscribe('price-change', partition, {offset: offset}, dataHandler)
    else
      return consumer.subscribe('price-change', dataHandler);
});