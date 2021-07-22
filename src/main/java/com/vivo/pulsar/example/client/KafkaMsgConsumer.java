package com.vivo.pulsar.example.client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author sikang.liu
 * @date 2021-03-19 14:10
 * @description
 */
public class KafkaMsgConsumer {

    public static final String DOMAIN_NAME = "10.101.50.13:59092,10.101.50.14:59092,10.101.50.15:59092";

    public static final String TOPIC_NAME = "persistent://lsk_tenant/lsk_ns/topic3p";

    public static final String CON_CLIENT_ID = "lsk_con";

    public static final String GROUP_NAME = "lsk_sub";

    public static final String TENANT = "lsk_tenant/lsk_ns";
    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuc19hZG1pbiJ9.uSoPDJClvMnXpCB5f8JLBLPLvP-_Ug6NPxHuPIAIgKM";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", DOMAIN_NAME);
        props.put("group.id", GROUP_NAME);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CON_CLIENT_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

        String jaasCfg = String.format(jaasTemplate, TENANT, PASSWORD);
        props.put("sasl.jaas.config", jaasCfg);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
