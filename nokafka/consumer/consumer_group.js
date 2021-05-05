var Kafka = require('no-kafka');
var Promise = require('bluebird');

var consumer = new Kafka.GroupConsumer({
    connectionString: '127.0.0.1:9092',
    groupId: 'group-consumer',
    clientId: process.argv[2],
    logger: {
        logFunction: console.log
    }
  });

var dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, function (m){
        console.log("ClientId: " + consumer.options.clientId, ", Topic: " + topic, ", Partition: " + partition, ", Offset: " + m.offset, 
            ", Message: " + m.message.value.toString('utf8'));
        return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
};

var strategies = [{
    subscriptions: ['price-change'],
    strategy: new Kafka.DefaultAssignmentStrategy(),
    handler: dataHandler
}];

consumer.init(strategies);