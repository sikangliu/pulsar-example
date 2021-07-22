package com.vivo.pulsar.example.client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

/**
 * @author sikang.liu
 * @date 2021-03-19 14:11
 * @description
 */
public class KafkaMsgProducer {

    public static final String DOMAIN_NAME = "10.101.50.13:59092,10.101.50.14:59092,10.101.50.15:59092";

    public static final String TOPIC_NAME = "persistent://lsk_tenant/lsk_ns/topic3p";

    public static final String PRO_CLIENTID = "lsk_pro";

    public static final String TENANT = "lsk_tenant/lsk_ns";
    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuc19hZG1pbiJ9.uSoPDJClvMnXpCB5f8JLBLPLvP-_Ug6NPxHuPIAIgKM";

    public static void main(String[] args) {
        Producer<String, String> producer = null;
        int sendCount=10;
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", DOMAIN_NAME);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, PRO_CLIENTID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

            String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
            String jaasCfg = String.format(jaasTemplate, TENANT, PASSWORD);
            props.put("sasl.jaas.config", jaasCfg);
            props.put("acks", "-1");

            producer = new KafkaProducer<>(props);
            for (int i = 0; i < sendCount; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, Integer.toString(i), Integer.toString(i));
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
