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

    public ClientBuild() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://10.101.50.13:6654,10.101.50.14:6654,10.101.50.15:6654")
                //pulsar-admin:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.OX2P8rYrvuX0AMf98pG8YzHdZKxHdhseni3ET_Y1WAk
                //ns_admin:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuc19hZG1pbiJ9.uSoPDJClvMnXpCB5f8JLBLPLvP-_Ug6NPxHuPIAIgKM
                //lsk:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJsc2sifQ.G_Xg5LepA175PlEKPLiCs7Es6T_pOu3T1tNzTjeV11Y
                .authentication(
                        AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.OX2P8rYrvuX0AMf98pG8YzHdZKxHdhseni3ET_Y1WAk"))
                .build();
    }

    public PulsarClient getPulsarClient(){
        return client;
    }
}
