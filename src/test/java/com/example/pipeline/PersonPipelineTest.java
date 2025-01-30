package com.example.pipeline;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PersonPipelineTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine")
    );

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.1")
    );

    @Test
    void shouldProcessPersonDataFromPostgresToKafka() throws Exception {
        // Create the persons table and insert test data
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("""
                CREATE TABLE persons (
                    id BIGINT PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    age INTEGER
                )
            """);

            stmt.execute("""
                INSERT INTO persons (id, first_name, last_name, email, age) VALUES
                (1, 'John', 'Doe', 'john.doe@example.com', 30),
                (2, 'Jane', 'Smith', 'jane.smith@example.com', 25),
                (3, 'Bob', 'Johnson', 'bob.johnson@example.com', 35)
            """);
        }

        // Run the pipeline
        String[] args = {
            "--postgresqlUrl=" + postgres.getJdbcUrl(),
            "--postgresqlUsername=" + postgres.getUsername(),
            "--postgresqlPassword=" + postgres.getPassword(),
            "--kafkaBootstrapServers=" + kafka.getBootstrapServers(),
            "--kafkaTopic=persons"
        };

        PersonPipeline.main(args);

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
            
            List<String> messages = new ArrayList<>();
            int maxAttempts = 10;
            int attempt = 0;

            while (messages.size() < 3 && attempt < maxAttempts) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    messages.add(record.value());
                }
                attempt++;
            }

            // Verify the messages
            assertThat(messages).hasSize(3);
            assertThat(messages).anySatisfy(message -> 
                assertThat(message).contains("John", "Doe", "john.doe@example.com")
            );
            assertThat(messages).anySatisfy(message -> 
                assertThat(message).contains("Jane", "Smith", "jane.smith@example.com")
            );
            assertThat(messages).anySatisfy(message -> 
                assertThat(message).contains("Bob", "Johnson", "bob.johnson@example.com")
            );
        }
    }
}
