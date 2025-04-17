package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import org.apache.kafka.clients.consumer.Consumer;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerExample {

    public static void main(final String[] args) throws Exception  {

        Properties props = loadKafkaProps();

        final String topic = "poems";

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String company = record.key();
                    String employee = record.value();
                    System.out.println("Consumer has consumed an event from topic: " +topic+ "as a key: " +company+ " and value: "+employee);
                }
            }
        }
    }

    public static Properties loadKafkaProps() throws IOException {
        Properties credentials = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            credentials.load(fis);
        }

        final String bootstrapServer = credentials.getProperty("bootstrapserver");
        final String username = credentials.getProperty("username");
        final String password = credentials.getProperty("password");

        // Kafka configuration
        Properties props = new Properties();

        // User-specific properties
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            username, password
        ));

        // Fixed properties
        props.put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getCanonicalName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(GROUP_ID_CONFIG,                 "kafka-java-demo");
        props.put(AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(SECURITY_PROTOCOL_CONFIG,        "SASL_SSL");
        props.put(SASL_MECHANISM,                  "PLAIN");

        return props;
    }
}