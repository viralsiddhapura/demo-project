package io.confluent.developer;

import java.io.FileReader;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ProducerAvro {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "pkc-p11xm.us-east-1.aws.confluent.cloud:9092");
        props.setProperty("key.serializer", IntegerSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "https://psrc-1ryj35w.us-east-1.aws.confluent.cloud");

        String topic = "avroDemo";

        KafkaProducer<Integer, AlienAttack> producer = new KafkaProducer<Integer, AlienAttack>(props);
        try {
            FileReader filereader = new FileReader("src/main/resources/alien_attack_events.csv");
            CSVReader csvReader = new CSVReaderBuilder(filereader)
                    .withSkipLines(1)
                    .build();
            List<String[]> allData = csvReader.readAll();

            for (String[] row : allData) {
                AlienAttack attack = AlienAttack.newBuilder()
                        .setHumanName(row[0])
                        .setPlanetVisited(row[1])
                        .setAttackDurationMinutes(Integer.parseInt(row[2]))
                        .setProbeUsed(Boolean.parseBoolean(row[3]))
                        .setAlienSpecies(row[4])
                        .build();

                int id = Integer.parseInt(row[0]);
                ProducerRecord<Integer, AlienAttack> pr = new ProducerRecord<Integer, AlienAttack>(topic, id, attack);
                RecordMetadata metadata = producer.send(pr).get();
                System.out.println(metadata);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.flush();
        producer.close();
    }
}
