const { Kafka } = require('kafkajs');

(async () => {
    const kafka = new Kafka({
        clientId: 'iot-msk-producer',
        brokers: [
            "b-2.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-1.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-3.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
        ],
        ssl: true
    });

    const consumer = kafka.consumer({ groupId: 'iot-data-consumer' })

    await consumer.connect()
    await consumer.subscribe({ topic: 'iot-data', fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: JSON.parse(message),
                topic,
                partition
            })
        },
    })

})()