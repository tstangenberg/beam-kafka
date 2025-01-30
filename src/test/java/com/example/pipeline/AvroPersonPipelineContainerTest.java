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
import org.junit.jupiter.api.Test;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

class AvroPersonPipelineContainerTest extends AbstractPersonPipelineContainerTest {

  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://test-registry";

  @Test
  void shouldProcessPersonDataFromPostgresToKafkaAsAvro() throws Exception {
    // Run the pipeline with Avro output
    String[] args = getBasePipelineArgs();
    String[] avroArgs = new String[args.length + 2];
    System.arraycopy(args, 0, avroArgs, 0, args.length);
    avroArgs[args.length] = "--outputFormat=avro";
    avroArgs[args.length + 1] = "--schemaRegistryUrl=" + MOCK_SCHEMA_REGISTRY_URL;

    PersonPipeline.main(avroArgs);

    // Configure Kafka consumer with Schema Registry
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");

    // Read messages from Kafka
    try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList("persons"));

      ConsumerRecord<String, GenericRecord> record =
          consumer.poll(Duration.ofSeconds(10)).iterator().next();

      GenericRecord person = record.value();
      assertThat(person.get("firstName").toString()).isEqualTo("John");
      assertThat(person.get("lastName").toString()).isEqualTo("Doe");
      assertThat(person.get("email").toString()).isEqualTo("john.doe@example.com");
      assertThat(person.get("age")).isEqualTo(30);
    }
  }
}
