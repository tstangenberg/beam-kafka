package com.example.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.beam.sdk.coders.SerializableCoder;

public class PersonPipeline {

    public interface Options extends PipelineOptions {
        @Description("PostgreSQL connection URL")
        @Default.String("jdbc:postgresql://localhost:5432/mydatabase")
        String getPostgresqlUrl();
        void setPostgresqlUrl(String value);

        @Description("PostgreSQL username")
        @Default.String("postgres")
        String getPostgresqlUsername();
        void setPostgresqlUsername(String value);

        @Description("PostgreSQL password")
        @Default.String("postgres")
        String getPostgresqlPassword();
        void setPostgresqlPassword(String value);

        @Description("Kafka bootstrap servers")
        @Default.String("localhost:9092")
        String getKafkaBootstrapServers();
        void setKafkaBootstrapServers(String value);

        @Description("Kafka topic")
        @Default.String("persons")
        String getKafkaTopic();
        void setKafkaTopic(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        // Create ObjectMapper for JSON serialization
        final ObjectMapper objectMapper = new ObjectMapper();

        // Read from PostgreSQL
        pipeline
            .apply("Read from PostgreSQL",
                JdbcIO.<Person>read()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "org.postgresql.Driver",
                        options.getPostgresqlUrl()
                    )
                    .withUsername(options.getPostgresqlUsername())
                    .withPassword(options.getPostgresqlPassword()))
                    .withQuery("SELECT id, first_name, last_name, email, age FROM persons")
                    .withRowMapper(resultSet -> new Person(
                        resultSet.getLong("id"),
                        resultSet.getString("first_name"),
                        resultSet.getString("last_name"),
                        resultSet.getString("email"),
                        resultSet.getInt("age")
                    )).withCoder(SerializableCoder.of(Person.class))                    
            )
            // Convert Person objects to JSON strings
            .apply("Convert to JSON", MapElements
                .into(TypeDescriptor.of(String.class))
                .via(person -> {
                    try {
                        return objectMapper.writeValueAsString(person);
                    } catch (Exception e) {
                        throw new RuntimeException("Error serializing person to JSON", e);
                    }
                }))
            // Write to Kafka
            .apply("Write to Kafka",
                KafkaIO.<Void, String>write()
                    .withBootstrapServers(options.getKafkaBootstrapServers())
                    .withTopic(options.getKafkaTopic())
                    .withValueSerializer(StringSerializer.class)
                    .values()
            );

        pipeline.run().waitUntilFinish();
    }
}
