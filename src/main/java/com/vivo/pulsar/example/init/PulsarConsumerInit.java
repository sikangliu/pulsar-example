package com.vivo.pulsar.example.init;

import com.vivo.pulsar.example.kop.KopConstant;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 15:31
 */
@Slf4j
public class PulsarConsumerInit {

    private final String topic;
    private volatile Consumer<String> consumer;

    public PulsarConsumerInit(String topic) {
        this.topic = topic;
    }

    public void init() {
        try {
            final PulsarClientInit instance = PulsarClientInit.getInstance();
            instance.init();

            consumer = instance.getPulsarClient().newConsumer(Schema.STRING)
                    .topic(topic)
                    .subscriptionName(KopConstant.SUBSCRIPTION_NAME)
                    .ackTimeout(10, TimeUnit.SECONDS)
                    .subscriptionType(SubscriptionType.Failover)
                    .consumerName(KopConstant.CONSUMER_NAME)
                    .receiverQueueSize(2000)
                    .messageListener(new MessageListener<>())
                    .subscribe();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Consumer<String> getConsumer() {
        return consumer;
    }
}
