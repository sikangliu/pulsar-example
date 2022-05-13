package com.vivo.pulsar.example.utils;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @author sikang.liu
 * @date 2021-03-19 11:23
 * @description
 */
public class ClientBuild {

    private PulsarClient client;

    public ClientBuild(String env) throws PulsarClientException {
        if (env.equals("local")) {
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
//                    .authentication(
//                            AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.AV_jbPsAXb3rnhDm8-zOVt_Zwi_3jX-XNXMw_MU6WNU"))
                    .build();
        } else {
            client = PulsarClient.builder()
//                    .serviceUrl("pulsar://10.101.50.13:6654,10.101.50.14:6654,10.101.50.15:6654")
//                    .authentication(
//                            AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXJhZG1pbiJ9.zSPFdsq2cA1KAveDf5AHGMp_ts8yWcrZORtl4_8J_NM"))
                    .build();
        }

    }

    public PulsarClient getPulsarClient() {
        return client;
    }
}
