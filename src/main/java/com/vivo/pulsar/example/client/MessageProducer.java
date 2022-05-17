package com.vivo.pulsar.example.client;

import com.vivo.pulsar.example.utils.ClientBuild;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.TimeUnit;

/**
 * @author sikang.liu
 * @date 2021-03-19 11:24
 * @description
 */
public class MessageProducer {

    PulsarClientInit instance;
    private Producer<String> producer;

    public MessageProducer(String topic) throws Exception {
        instance = PulsarClientInit.getInstance();
        producer = createProducer(topic);
    }

    private Producer<String> createProducer(String topic) throws Exception {
        return instance.getPulsarClient().newProducer(Schema.STRING)
                .topic(topic)
                .producerName("lsk_pro")
                .enableBatching(true)
                .batchingMaxBytes(50)
                .sendTimeout(0, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
    }

    public void sendMessage(String message) {
        producer.sendAsync(message).thenAccept(msgId -> {
            System.out.println("消息ID为: " + msgId + "发送成功");
        });
    }

    public void close() {
        producer.closeAsync().thenRun(() -> System.out.println("Producer closed"));
    }

    public static void main(String[] args) {
        int sendCount = 10;
        String topicName = "persistent://kop/kop-ns/kop-test";
        MessageProducer producer = null;
        try {
            producer = new MessageProducer(topicName);
            for (int i = 1; i <= sendCount; i++) {
                String message = "pulsar客户端发送的第" + i + "条消息";
                producer.sendMessage(message);
                Thread.sleep(1);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            producer.close();
            System.exit(0);
        }

    }
}
