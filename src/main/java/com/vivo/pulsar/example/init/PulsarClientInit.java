package com.vivo.pulsar.example.init;

import com.vivo.pulsar.example.kop.KopConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 10:27
 */
@Slf4j
public class PulsarClientInit {

    private static final PulsarClientInit INSTANCE = new PulsarClientInit();
    private volatile PulsarClient pulsarClient;

    public static PulsarClientInit getInstance() {
        return INSTANCE;
    }

    public void init() {
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(KopConstant.SERVICE_URL)
                    .authentication(AuthenticationFactory.token(KopConstant.TOKEN))
                    .build();
            log.info("pulsar client init success");
        } catch (Exception e) {
            log.error("init pulsar error, exception is ", e);
        }
    }

    public PulsarClient getPulsarClient() {
        return pulsarClient;
    }
}
