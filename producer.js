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

    // const producer = kafka.producer()
    const admin = kafka.admin();
    await admin.connect();

    const cluster = await admin.describeCluster();
    console.log(cluster);

    try {
        await admin.createTopics({
            waitForLeaders: false,
            topics: [{ topic: "test-topic", numPartitions: 1, replicationFactor: 1 }],
        })

    } catch (error) {
        console.log(error);
    }

    // await producer.connect()

    // await producer.send({
    //     topic: 'test-topic',
    //     messages: [
    //         { value: 'Hello KafkaJS user!1' },
    //         { value: 'Hello KafkaJS user!2' },
    //         { value: 'Hello KafkaJS user!3' },
    //         { value: 'Hello KafkaJS user!4' },
    //         { value: 'Hello KafkaJS user!5' },
    //         { value: 'Hello KafkaJS user!6' },
    //     ],
    // })
    // await producer.disconnect();
    await admin.disconnect();
    console.log("done");

})()