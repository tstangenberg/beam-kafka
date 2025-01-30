package com.example.pipeline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class PersonPipeline {

  public interface PersonPipelineOptions extends PipelineOptions {
    @Description("PostgreSQL JDBC URL")
    String getPostgresqlUrl();

    void setPostgresqlUrl(String value);

    @Description("PostgreSQL username")
    String getPostgresqlUsername();

    void setPostgresqlUsername(String value);

    @Description("PostgreSQL password")
    String getPostgresqlPassword();

    void setPostgresqlPassword(String value);

    @Description("Kafka bootstrap servers")
    String getKafkaBootstrapServers();

    void setKafkaBootstrapServers(String value);

    @Description("Kafka topic")
    String getKafkaTopic();

    void setKafkaTopic(String value);

    @Description("Output format (json or avro)")
    @Default.String("json")
    String getOutputFormat();

    void setOutputFormat(String value);

    @Description("Schema Registry URL (optional)")
    @Default.String("")
    String getSchemaRegistryUrl();

    void setSchemaRegistryUrl(String value);
  }

  private static final org.apache.avro.Schema AVRO_SCHEMA =
      org.apache.avro.Schema.parse(
          "{"
              + "\"type\": \"record\","
              + "\"name\": \"Person\","
              + "\"namespace\": \"com.example.pipeline\","
              + "\"fields\": ["
              + "  {\"name\": \"firstName\", \"type\": \"string\"},"
              + "  {\"name\": \"lastName\", \"type\": \"string\"},"
              + "  {\"name\": \"email\", \"type\": \"string\"},"
              + "  {\"name\": \"age\", \"type\": \"int\"}"
              + "]}");

  private static Map<String, Object> getKafkaConfig(PersonPipelineOptions options) {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", options.getKafkaBootstrapServers());

    if ("avro".equals(options.getOutputFormat()) && !options.getSchemaRegistryUrl().isEmpty()) {
      config.put("schema.registry.url", options.getSchemaRegistryUrl());
      return config;
    }

    return config;
  }

  public static void main(String[] args) {
    PersonPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PersonPipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    // Create ObjectMapper for JSON serialization
    final ObjectMapper objectMapper = new ObjectMapper();

    org.apache.beam.sdk.schemas.Schema beamSchema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addStringField("first_name")
            .addStringField("last_name")
            .addStringField("email")
            .addInt32Field("age")
            .build();

    PCollection<Row> personRows =
        pipeline.apply(
            "Read from PostgreSQL",
            JdbcIO.<Row>read()
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(
                            "org.postgresql.Driver", options.getPostgresqlUrl())
                        .withUsername(options.getPostgresqlUsername())
                        .withPassword(options.getPostgresqlPassword()))
                .withQuery("SELECT first_name, last_name, email, age FROM persons")
                .withRowMapper(
                    resultSet ->
                        Row.withSchema(beamSchema)
                            .addValues(
                                resultSet.getString("first_name"),
                                resultSet.getString("last_name"),
                                resultSet.getString("email"),
                                resultSet.getInt("age"))
                            .build())
                .withCoder(SerializableCoder.of(Row.class)));

    PCollection<KV<String, byte[]>> serializedRows =
        personRows.apply(
            "Serialize to bytes",
            ParDo.of(
                new DoFn<Row, KV<String, byte[]>>() {
                  private transient ObjectMapper objectMapper;
                  private transient KafkaAvroSerializer kafkaAvroSerializer;

                  @Setup
                  public void setup() {
                    objectMapper = new ObjectMapper();
                  }

                  @StartBundle
                  public void startBundle(StartBundleContext c) {
                    PersonPipelineOptions options =
                        c.getPipelineOptions().as(PersonPipelineOptions.class);
                    if ("avro".equals(options.getOutputFormat())) {
                      Map<String, String> config = new HashMap<>();
                      config.put("schema.registry.url", options.getSchemaRegistryUrl());
                      kafkaAvroSerializer = new KafkaAvroSerializer();
                      kafkaAvroSerializer.configure(config, false);
                    }
                  }

                  @ProcessElement
                  public void processElement(
                      @Element Row row, OutputReceiver<KV<String, byte[]>> out, ProcessContext c)
                      throws IOException {
                    PersonPipelineOptions options =
                        c.getPipelineOptions().as(PersonPipelineOptions.class);
                    String key = row.getString("email");

                    if ("json".equals(options.getOutputFormat())) {
                      Map<String, Object> jsonMap = new HashMap<>();
                      jsonMap.put("firstName", row.getString("first_name"));
                      jsonMap.put("lastName", row.getString("last_name"));
                      jsonMap.put("email", row.getString("email"));
                      jsonMap.put("age", row.getInt32("age"));
                      byte[] value = objectMapper.writeValueAsBytes(jsonMap);
                      out.output(KV.of(key, value));
                    } else if ("avro".equals(options.getOutputFormat())) {
                      GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
                      record.put("firstName", row.getString("first_name"));
                      record.put("lastName", row.getString("last_name"));
                      record.put("email", row.getString("email"));
                      record.put("age", row.getInt32("age"));
                      byte[] value = kafkaAvroSerializer.serialize("persons", record);
                      out.output(KV.of(key, value));
                    }
                  }

                  @Teardown
                  public void teardown() {
                    if (kafkaAvroSerializer != null) {
                      kafkaAvroSerializer.close();
                    }
                  }
                }));

    // Write to Kafka
    serializedRows.apply(
        "Write to Kafka",
        KafkaIO.<String, byte[]>write()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withTopic(options.getKafkaTopic())
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(ByteArraySerializer.class));

    pipeline.run().waitUntilFinish();
  }
}
