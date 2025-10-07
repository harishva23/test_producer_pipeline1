import { Kafka } from "kafkajs";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

const kafka = new Kafka({
  clientId: "sample-protobuf-producer",
  brokers: ["my-cluster-kafka-bootstrap.kafka.svc:9092"],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.SASL_USERNAME,
    password: process.env.SASL_PASSWORD,
  },
});

const registry = new SchemaRegistry({
  host: "http://karapace-schema-registry.schema-registry:8081",
});

const producer = kafka.producer();

const TOPIC = process.env.TOPIC;
const SUBJECT = process.env.SUBJECT;

const run = async () => {
  await producer.connect();

  const { id } = await registry.getLatestSchemaId(SUBJECT);
  console.log(`Using schema ID ${id} for subject ${SUBJECT}`);

  const sendMessage = async () => {
    const payload = {
      producerId: 1,
      time: new Date().toISOString(),
    };

    const encodedValue = await registry.encode(id, payload);

    await producer.send({
      topic: TOPIC,
      messages: [{ value: encodedValue }],
    });

    console.log("Produced:", payload);
  };

  setInterval(sendMessage, 5000);
};

run().catch((err) => {
  console.error("Producer error:", err);
});
