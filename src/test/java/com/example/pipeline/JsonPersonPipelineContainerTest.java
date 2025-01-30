package com.example.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.TesSt;

class JsonPersonPipelineContainerTest extends AbstractPersonPipelineContainerTest {

    @Test
    void shouldProcessPersonDataFromPostgresToKafkaAsJson() throws Exception {
        // Run the pipeline with JSON output
        String[] args = getBasePipelineArgs();
        String[] jsonArgs = new String[args.length + 1];
        System.arraycopy(args, 0, jsonArgs, 0, args.length);
        jsonArgs[args.length] = "--outputFormat=json";
        
        PersonPipeline.main(jsonArgs);

        // Configure Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Read messages from Kafka
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("persons"));
            
            ConsumerRecord<String, String> record = consumer.poll(Duration.ofSeconds(10))
                .iterator()
                .next();

            // Verify the JSON message
            assertThat(record.value())
                .contains("John")
                .contains("Doe")
                .contains("john.doe@example.com")
                .contains("30");
        }
    }
}
