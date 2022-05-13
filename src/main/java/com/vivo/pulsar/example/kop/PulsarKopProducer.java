package com.vivo.pulsar.example.kop;

import com.vivo.pulsar.example.init.PulsarProducerInit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 15:59
 */
@Slf4j
public class PulsarKopProducer {

    public static void main(String[] args) throws PulsarClientException {

        Producer<String> producer = null;
        try {
            PulsarProducerInit producerInit = new PulsarProducerInit(KopConstant.TOPIC_NAME);
            producerInit.init();
            producer = producerInit.getProducer();

            int sendCount = 10;
            for (int i = 1; i <= sendCount; i++) {
                String message = "pulsar客户端发送的第" + i + "条消息";

                producer.sendAsync(message)
                        .thenAccept(msgId -> System.out.println("消息ID为: " + msgId + "发送成功"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.exit(0);
        }

    }

}
