package com.example.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

class SchemaRegistryPersonPipelineContainerTest extends AbstractPersonPipelineContainerTest {

    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private GenericContainer<?> schemaRegistry;

    @BeforeEach
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.3.0"))
                .withNetwork(getNetwork())
                .withExposedPorts(SCHEMA_REGISTRY_PORT)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        "PLAINTEXT://" + getKafka().getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
    }

    @AfterEach
    @Override
    protected void tearDown() throws Exception {
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        super.tearDown();
    }

    @Test
    void shouldProcessPersonDataWithSchemaRegistry() throws Exception {
        schemaRegistry.start();
        String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":"
                + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT);

        // Run the pipeline with Schema Registry
        String[] args = getBasePipelineArgs();
        String[] avroArgs = new String[args.length + 2];
        System.arraycopy(args, 0, avroArgs, 0, args.length);
        avroArgs[args.length] = "--outputFormat=avro";
        avroArgs[args.length + 1] = "--schemaRegistryUrl=" + schemaRegistryUrl;

        PersonPipeline.main(avroArgs);

        // Configure Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");

        // Read messages from Kafka
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("persons"));

            ConsumerRecord<String, GenericRecord> record = consumer.poll(Duration.ofSeconds(10))
                    .iterator()
                    .next();

            GenericRecord person = record.value();
            assertThat(person.get("firstName").toString()).isEqualTo("John");
            assertThat(person.get("lastName").toString()).isEqualTo("Doe");
            assertThat(person.get("email").toString()).isEqualTo("john.doe@example.com");
            assertThat(person.get("age")).isEqualTo(30);
        }
    }
}
