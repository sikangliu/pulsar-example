package com.vivo.pulsar.example.client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author sikang.liu
 * @date 2021-03-19 14:10
 * @description
 */
public class KafkaMsgConsumer {

//    public static final String DOMAIN_NAME = "localhost:9092";
//    public static final String TOPIC_NAME = "persistent://kop-tn/kop-ns/kop-test";
//    public static final String CON_CLIENT_ID = "kop_con";
//    public static final String GROUP_NAME = "kop-sub";
//    public static final String TENANT = "kop-tn/kop-ns";
//    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.AV_jbPsAXb3rnhDm8-zOVt_Zwi_3jX-XNXMw_MU6WNU";

    public static final String DOMAIN_NAME = "10.101.129.65:9092";
    public static final String TOPIC_NAME = "persistent://kop/kop-ns/kop-test";
    public static final String CON_CLIENT_ID = "kop_con";
    public static final String GROUP_NAME = "kop-sub";
    public static final String TENANT = "kop/kop-ns";
    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJrb3Atcm9sZSJ9.8rU1Nj-KAViLLurdz7Cdoq3hxS8rY-XJba0Ff6ulYho";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", DOMAIN_NAME);
        props.put("group.id", GROUP_NAME);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CON_CLIENT_ID);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");

        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
//        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

        String jaasCfg = String.format(jaasTemplate, TENANT, PASSWORD);
        props.put("sasl.jaas.config", jaasCfg);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            // 设置 1s 中消费一批数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            // 打印消费到的数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
