package com.vivo.pulsar.example.kop;

import com.vivo.pulsar.example.init.PulsarConsumerInit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.ExecutionException;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 16:38
 */
public class PulsarKopConsumer {

    public static void main(String[] args) {

        PulsarConsumerInit consumerInit = new PulsarConsumerInit(KopConstant.TOPIC_NAME);
        consumerInit.init();
        Consumer<String> consumer = consumerInit.getConsumer();

        consumer.receiveAsync();
    }
}
