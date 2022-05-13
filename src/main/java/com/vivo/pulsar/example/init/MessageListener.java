package com.vivo.pulsar.example.init;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 15:33
 */
@Slf4j
public class MessageListener<T> implements org.apache.pulsar.client.api.MessageListener<T> {

    @SneakyThrows
    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        System.out.println("Message received is: " + new String(msg.getData()));

        consumer.acknowledge(msg);
    }
}
