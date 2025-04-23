package io.confluent.developer;

import java.io.FileInputStream;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import org.apache.kafka.clients.producer.KafkaProducer;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import org.apache.kafka.common.serialization.IntegerSerializer;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ProducerAvro {

    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    public static void main(String[] args) throws Exception {
        Properties credentials = new Properties();

        try (FileInputStream fis = new FileInputStream("config.properties")) {
            credentials.load(fis);
        }

        Properties props = new Properties();

        final String bootstrapServer = credentials.getProperty("bootstrapserver");
        final String username = credentials.getProperty("username");
        final String password = credentials.getProperty("password");
        final String schemaRegistryUrl = credentials.getProperty("schema-registry-url");
        final String srApiKey = credentials.getProperty("sr_api_key");
        final String srApiSecret = credentials.getProperty("sr_secret_key");

        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);

        props.put(ACKS_CONFIG, "all");
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SASL_MECHANISM, "PLAIN");
        props.put(SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            username, password
        ));

        // Schema Registry authentication
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", String.format("%s:%s", srApiKey, srApiSecret));

        final String topic = "avroDemo";

        try (KafkaProducer<Integer, AlienAttack> producer = new KafkaProducer<>(props)) {
            try {
                FileReader filereader = new FileReader("src/main/resources/alien_attack_events.csv");
                CSVReader csvReader = new CSVReaderBuilder(filereader)
                        .withSkipLines(1)
                        .build();
                List<String[]> allData = csvReader.readAll();

                int id = 1;
                for (String[] row : allData) {
                    System.out.println(row[0]);
                    AlienAttack attack = AlienAttack.newBuilder()
                            .setHumanName(row[0])
                            .setPlanetVisited(row[1])
                            .setAttackDurationMinutes(Integer.parseInt(row[2]))
                            .setProbeUsed(Boolean.parseBoolean(row[3]))
                            .setAlienSpecies(row[4])
                            .build();

                    System.out.println(attack);
                    producer.send(
                        new ProducerRecord<>(topic, id, attack),
                        (recordMetadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.println("Producer has produced an event to topic: " + 
                                    topic + " with value: " + attack);
                            }
                        }
                    );
                    id++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
    }
}
