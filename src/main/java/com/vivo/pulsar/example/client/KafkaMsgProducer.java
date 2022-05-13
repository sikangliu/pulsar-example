package com.vivo.pulsar.example.client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author sikang.liu
 * @date 2021-03-19 14:11
 * @description
 */
public class KafkaMsgProducer {

    public static final String DOMAIN_NAME = "localhost:9092";
    public static final String TOPIC_NAME = "persistent://kop-tn/kop-ns/kop-test";
    public static final String PRO_CLIENT_ID = "kop_pro";
    public static final String TENANT = "kop-tn/kop-ns";
    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.AV_jbPsAXb3rnhDm8-zOVt_Zwi_3jX-XNXMw_MU6WNU";

//    public static final String DOMAIN_NAME = "10.101.129.65:9092";
//    public static final String TOPIC_NAME = "persistent://kop-tn/kop-ns/kop-test";
//    public static final String PRO_CLIENT_ID = "kop_pro";
//    public static final String TENANT = "kop-tn/kop-ns";
//    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.AV_jbPsAXb3rnhDm8-zOVt_Zwi_3jX-XNXMw_MU6WNU";

    public static void main(String[] args) {
        Producer<String, String> producer = null;
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", DOMAIN_NAME);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, PRO_CLIENT_ID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//            props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");

            String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
//            String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

            String jaasCfg = String.format(jaasTemplate, TENANT, PASSWORD);
            props.put("sasl.jaas.config", jaasCfg);
            props.put("acks", "1");

            producer = new KafkaProducer<>(props);
            for (int i = 0; i < 10; i++) {
                String message = "kafka客户端发送的第" + i + "条消息";
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, Integer.toString(i), message);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        // 没有异常,输出信息到控制台
                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition());
                    } else {
                        exception.printStackTrace();
                    }
                });
                // 延迟一会会看到数据发往不同分区
                Thread.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.exit(0);
        }
    }
}
