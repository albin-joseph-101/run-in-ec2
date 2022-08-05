const { Kafka, PartitionAssigners } = require('kafkajs');

(async () => {
    const kafka = new Kafka({
        clientId: 'iot-msk-producer',
        brokers: [
            "b-2.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-1.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-3.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
        ],
        ssl: true,
        retry: 30
    });

    const admin = kafka.admin();
    await admin.connect();

    const cluster = await admin.describeCluster();
    console.log(cluster);

    try {
        const topicjs = await admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic: "test-topic-123", numPartitions: 3, replicationFactor: 3 }],
        })
        console.log({ topicjs });
    } catch (error) {
        console.log(error);
    }



    const producer = kafka.producer({
        allowAutoTopicCreation: true,
        retry: 30
    })

    await producer.connect()

    const res = await producer.send({
        topic: 'test-topic-2',
        messages: [
            { value: 'Hello KafkaJS user!1' },
            { value: 'Hello KafkaJS user!2' },
            { value: 'Hello KafkaJS user!3' },
            { value: 'Hello KafkaJS user!4' },
            { value: 'Hello KafkaJS user!5' },
            { value: 'Hello KafkaJS user!6' },
        ],
    })
    console.log(res);
    await producer.disconnect();
    // await admin.disconnect();
    console.log("done");

})()