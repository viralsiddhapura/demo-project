package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerExample {

    @SuppressWarnings("CallToPrintStackTrace")
    public static void main(final String[] args) throws Exception {

        Properties props = loadKafkaProps();

        final String topic = "poems";

        String[] companies = {"Psyncopate", "City National Bank", "World Vision Internaitonal", "Salesforce", "Pacefic Dental Services", "Cross River", "Team viewer", "Record Point"};
        String[] employees = {"Tony", "Phoutty", "Ariahnt", "Suganya", "Faizal", "Rishabh"};
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random randumNumber = new Random();
            final int numberOfMessages = 10;
            for (int i = 0; i < numberOfMessages; i++) {
                String company = companies[randumNumber.nextInt(companies.length)];
                String employee = employees[randumNumber.nextInt(employees.length)];

                producer.send(
                        new ProducerRecord<>(topic, company, employee),
                        (event, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.println("Producer has produced an event to topic: " +topic+ "with a key: " +company+ " and value: "+employee);
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numberOfMessages, topic);
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
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ACKS_CONFIG, "all");
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SASL_MECHANISM, "PLAIN");

        return props;
    }
}