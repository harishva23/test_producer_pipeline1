import { Kafka } from "kafkajs";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import protobuf from "protobufjs";

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
const SUBJECT = process.env.SUBJECT; // Karapace subject name

// Load the Protobuf schema dynamically
const root = await protobuf.load("SampleProducer.proto");
const SampleProducer = root.lookupType("SampleProducer");

const run = async () => {
  await producer.connect();

  // Get the latest schema ID from Karapace
  const { id: schemaId } = await registry.getLatestSchema(SUBJECT);
  console.log("Using schema ID:", schemaId);

  // Function to produce a message
  const sendMessage = async () => {
    const payload = SampleProducer.create({
      producerId: 1,
      time: new Date().toISOString(),
    });

    const buffer = SampleProducer.encode(payload).finish();

    // Encode for Karapace schema registry
    const encodedValue = await registry.encode(schemaId, buffer);

    await producer.send({
      topic: TOPIC,
      messages: [{ value: encodedValue }],
    });

    console.log("Produced:", payload);
  };

  // Produce every 5 seconds
  setInterval(sendMessage, 5000);
};

run().catch(console.error);
