const { Kafka } = require('kafkajs');

const processMessage = async (message) => {
    const body = new Buffer(message.value, 'base64').toString();
    console.log(body);
}

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
    await consumer.subscribe({ topic: 'iot-data-stream', fromBeginning: true })
    await consumer.run({
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
            for (let message of batch.messages) {
                if (!isRunning() || isStale())
                    break;
                await processMessage(message);
                await resolveOffset(message.offset);
                await heartbeat();
            }
        }
    })

})()