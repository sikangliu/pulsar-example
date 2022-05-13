package com.vivo.pulsar.example.init;

import com.vivo.pulsar.example.kop.KopConstant;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 15:26
 */
@Slf4j
public class PulsarProducerInit {

    private final String topic;
    private volatile Producer<String> producer;

    public PulsarProducerInit(String topic) {
        this.topic = topic;
    }

    public void init() {
        try {
            final PulsarClientInit instance = PulsarClientInit.getInstance();
            instance.init();

            producer = instance.getPulsarClient().newProducer(Schema.STRING)
                    .topic(topic)
                    .producerName(KopConstant.PRODUCER_NAME)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .blockIfQueueFull(true)
                    .create();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Producer<String> getProducer() {
        return producer;
    }
}
