const { Kafka } = require('kafkajs');

(async () => {
    const kafka = new Kafka({
        clientId: 'iot-msk-producer',
        brokers: [
            "b-2.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-1.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-3.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
        ],
    });

    const consumer = kafka.consumer({ groupId: 'test-group' })

    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic-123', fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
                topic,
                partition
            })
        },
    })

})()