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
const SUBJECT = process.env.SUBJECT; // Match your Karapace subject exactly

const run = async () => {
  await producer.connect();

  // Fetch latest Protobuf schema (works reliably with Karapace)
  const { schema, id } = await registry.getLatestSchema(SUBJECT);
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
