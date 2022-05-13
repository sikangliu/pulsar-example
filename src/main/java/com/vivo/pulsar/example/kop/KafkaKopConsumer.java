package com.vivo.pulsar.example.kop;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Arrays;
import java.util.Properties;

public class KafkaKopConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KopConstant.DOMAIN_NAME);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KopConstant.GROUP_NAME);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, KopConstant.CON_CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KopConstant.DESERIALIZATION);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KopConstant.DESERIALIZATION);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, KopConstant.TENANT, KopConstant.PASSWORD);
        props.put("sasl.jaas.config", jaasCfg);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(KopConstant.TOPIC_NAME));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
