const { Kafka } = require('kafkajs');

(async () => {
    const kafka = new Kafka({
        clientId: 'iot-msk-producer',
        brokers: [
            "b-2.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-1.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
            "b-3.iotstream.rnnl6v.c6.kafka.us-east-2.amazonaws.com:9094",
        ],
        ssl: true,
    });

    const admin = kafka.admin();
    await admin.connect();

    // const cluster = await admin.describeCluster();
    // const metadata = await admin.fetchTopicMetadata(["test-topic-123", "test-topic", "test-topic-1"])
    // console.log({ cluster: JSON.stringify(cluster), metadata: JSON.stringify(metadata) });

    try {
        // const topicjs = await admin.deleteTopics({
        //     topics: ["test-topic-123"],
        // })
        // await admin.deleteTopics({
        //     waitForLeaders: true,
        //     topics: [{ topic: "test-topic-123", numPartitions: 3, replicationFactor: 3 }],
        // })
        const topicjs = admin.createTopics({
            waitForLeaders: true,
            topics: [
                {
                    topic: "iot-data",
                    numPartitions: 1,
                    replicationFactor: 3,
                    replicaAssignment: [],
                    configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
                }
            ]
        })
        const metadata = await admin.fetchTopicMetadata()
        console.log({ topicjs, metadata: JSON.stringify(metadata) });
    } catch (error) {
        console.log(error);
    }



    // const producer = kafka.producer({
    //     allowAutoTopicCreation: true,
    //     retry: 30
    // })

    // await producer.connect()

    // const res = await producer.send({
    //     topic: 'test-topic-123',
    //     messages: [
    //         { value: 'Hello KafkaJS user!1' },
    //         { value: 'Hello KafkaJS user!2' },
    //         { value: 'Hello KafkaJS user!3' },
    //         { value: 'Hello KafkaJS user!4' },
    //         { value: 'Hello KafkaJS user!5' },
    //         { value: 'Hello KafkaJS user!6' },
    //         { value: 'Hello KafkaJS user!1' },
    //         { value: 'Hello KafkaJS user!2' },
    //         { value: 'Hello KafkaJS user!3' },
    //         { value: 'Hello KafkaJS user!4' },
    //         { value: 'Hello KafkaJS user!5' },
    //         { value: 'Hello KafkaJS user!6' },
    //     ],
    // })
    // console.log({res});
    // await producer.disconnect();
    await admin.disconnect();
    console.log("done");

})()