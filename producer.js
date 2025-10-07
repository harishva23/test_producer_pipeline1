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

const TOPIC = process.env.TOPIC;      // e.g., "sample-protobuf-topic"
const SUBJECT = process.env.SUBJECT;  // e.g., "sample-protobuf-topic-value"

const run = async () => {
  await producer.connect();

  // Fetch latest schema ID from Karapace
  const id = await registry.getLatestSchemaId(SUBJECT);
  if (!id) {
    console.error(`Schema ID not found for subject ${SUBJECT}`);
  }
  console.log("Using schema ID:", id);

  // Function to produce a message
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

  // Produce every 5 seconds
  setInterval(sendMessage, 5000);
};

// Run producer
run().catch((err) => {
  console.error("Producer error:", err);
});
