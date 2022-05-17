package com.vivo.pulsar.example.client;

import com.vivo.pulsar.example.utils.ClientBuild;
import org.apache.pulsar.client.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author sikang.liu
 * @date 2021-03-19 11:24
 * @description
 */
public class MessageConsumer {

    PulsarClientInit instance;
    private Consumer<String> consumer;

    public MessageConsumer(String topic, String subscription, String conName) throws Exception {
        instance = PulsarClientInit.getInstance();
        consumer = createConsumer(topic, subscription, conName);
        System.out.println("consumer is:" + consumer);
    }

    private Consumer createConsumer(String topic, String subscription, String conName) throws Exception {
        return instance.getPulsarClient().newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscription)
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Failover)
                .consumerName(conName)
                .subscribe();
    }

    public void receiveMessage() throws ExecutionException, InterruptedException, PulsarClientException {
        do {
            CompletableFuture<Message<String>> msg = consumer.receiveAsync();
            System.out.println("Message received is: " + new String(msg.get().getData()));

            consumer.acknowledge(msg.get());
        } while (true);
    }


    public static void main(String[] args) throws Exception {
        String topicName = "persistent://kop/kop-ns/kop-test";
        String subscription = "kop-sub";
        String conName = "lsk-con";
        MessageConsumer consumer = new MessageConsumer(topicName, subscription, conName);

        consumer.receiveMessage();
    }
}
