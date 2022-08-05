const { Kafka } = require('kafkajs');

(async () => {
    const kafka = new Kafka({
        clientId: 'iot-msk-producer',
        brokers: [
            "b-2.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9098",
            "b-1.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9098",
            "b-3.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9098",
        ],
        ssl: true
    });

    const producer = kafka.producer()
    const admin = kafka.admin();
    await admin.connect();

    await admin.createTopics({
        topics: [{
            topic: "test-topic",
            replicaAssignment: [{ partition: 0, replicas: [0] }],
            configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
        }]
    })

    await producer.connect()

    await producer.send({
        topic: 'test-topic',
        messages: [
            { value: 'Hello KafkaJS user!1' },
            { value: 'Hello KafkaJS user!2' },
            { value: 'Hello KafkaJS user!3' },
            { value: 'Hello KafkaJS user!4' },
            { value: 'Hello KafkaJS user!5' },
            { value: 'Hello KafkaJS user!6' },
        ],
    })
    console.log("done");
    await producer.disconnect();
    await admin.disconnect();

})()