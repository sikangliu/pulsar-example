package com.vivo.pulsar.example.kop;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class KafkaKopProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KopConstant.DOMAIN_NAME);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KopConstant.PRO_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KopConstant.SERIALIZATION);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KopConstant.SERIALIZATION);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, KopConstant.TENANT, KopConstant.PASSWORD);
        props.put("sasl.jaas.config", jaasCfg);
        props.put("acks", "1");

        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            int sendCount = 10;
            for (int i = 0; i < sendCount; i++) {
                String message = "kafka客户端发送的第" + i + "条消息";
                ProducerRecord<String, String> record = new ProducerRecord<>(KopConstant.TOPIC_NAME, Integer.toString(i), message);

                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.exit(0);
        }
    }
}
