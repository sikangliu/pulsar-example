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

    private ClientBuild client;
    private Consumer consumer;

    public MessageConsumer(String topic, String subscription, String conName) throws PulsarClientException {
        client = new ClientBuild();
        consumer = createConsumer(topic, subscription, conName);
        System.out.println("consumer is:" + consumer);
    }

    private Consumer createConsumer(String topic, String subscription, String conName) throws PulsarClientException {
        return client.getPulsarClient().newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscription)
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Failover)
                .consumerName(conName)
                .subscribe();
    }

    public void receiveMessage() throws ExecutionException, InterruptedException, PulsarClientException {
        do {
            CompletableFuture<Message> msg = consumer.receiveAsync();
            System.out.println("Message received is: " + new String(msg.get().getData()));

            consumer.acknowledge(msg.get());
        } while (true);
    }


    public static void main(String[] args) throws PulsarClientException, ExecutionException, InterruptedException {

        MessageConsumer consumer = new MessageConsumer("persistent://lsk_tenant/lsk_ns/topic3p", "lsk_sub", "lsk_con");

        consumer.receiveMessage();
    }
}
