package io.confluent.developer;

import java.io.FileInputStream;
import java.util.Properties;

public class ConsumerAvro {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        }

    }
}