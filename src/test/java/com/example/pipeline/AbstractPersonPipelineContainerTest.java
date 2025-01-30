package com.example.pipeline;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractPersonPipelineContainerTest {
    protected PostgreSQLContainer<?> postgres;
    protected KafkaContainer kafka;
    protected Network network;

    @BeforeEach
    protected void setUp() throws Exception {
        // Create network
        network = Network.newNetwork();

        // Start containers
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withNetwork(network);
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))
            .withNetwork(network)
            .withNetworkAliases("kafka");
        
        postgres.start();
        kafka.start();

        // Create table and insert test data
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("DROP TABLE IF EXISTS persons");
            stmt.execute("""
                CREATE TABLE persons (
                    id BIGSERIAL PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    age INTEGER
                )
            """);

            stmt.execute("""
                INSERT INTO persons (first_name, last_name, email, age) VALUES
                ('John', 'Doe', 'john.doe@example.com', 30)
            """);
        }
    }

    @AfterEach
    protected void tearDown() throws Exception {
        if (kafka != null) {
            kafka.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    protected String[] getBasePipelineArgs() {
        return new String[] {
            "--postgresqlUrl=" + postgres.getJdbcUrl(),
            "--postgresqlUsername=" + postgres.getUsername(),
            "--postgresqlPassword=" + postgres.getPassword(),
            "--kafkaBootstrapServers=" + kafka.getBootstrapServers(),
            "--kafkaTopic=persons"
        };
    }

    protected Network getNetwork() {
        return network;
    }

    protected KafkaContainer getKafka() {
        return kafka;
    }

    protected PostgreSQLContainer<?> getPostgres() {
        return postgres;
    }
}
