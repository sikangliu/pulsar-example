package com.vivo.pulsar.example.client;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-05-07 11:03
 */
public class PulsarClientInit {
    private static final PulsarClientInit INSTANCE = new PulsarClientInit();
    private PulsarClient pulsarClient;

    public static PulsarClientInit getInstance() {
        return INSTANCE;
    }

    public PulsarClient getPulsarClient() throws Exception {
        pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
//                .serviceUrl("pulsar://10.101.129.65:6650,10.101.129.68:6650,10.101.129.70:6650,10.101.129.75:6650")
                .authentication(
                        AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.AV_jbPsAXb3rnhDm8-zOVt_Zwi_3jX-XNXMw_MU6WNU"))
//                        AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJrb3Atcm9sZSJ9.8rU1Nj-KAViLLurdz7Cdoq3hxS8rY-XJba0Ff6ulYho"))
                .build();
        return pulsarClient;
    }
}
