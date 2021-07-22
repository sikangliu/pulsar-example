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

    private ClientBuild client;
    private Producer<String> producer;

    public MessageProducer(String topic) throws PulsarClientException {
        client = new ClientBuild();
        producer = createProducer(topic);
    }

    private Producer<String> createProducer(String topic) throws PulsarClientException {
        return client.getPulsarClient().newProducer(Schema.STRING)
                .topic(topic)
                .producerName("lsk_pro")
                .sendTimeout(0, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
    }

    public void sendMessage(String message) {
        producer.sendAsync(message).thenAccept(msgId -> {
            System.out.printf("Message with ID %s successfully sent", msgId);
        });
    }

    public void close() {
        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"));
    }

    public static void main(String[] args) throws PulsarClientException {
        int sendCount=10000;
        MessageProducer producer = new MessageProducer("persistent://lsk_tenant/lsk_ns/topic3p");
        for (int i = 1; i <= sendCount; i++) {
            producer.sendMessage("Hello World-"+i);
        }
        producer.close();
        System.exit(0);
    }
}
