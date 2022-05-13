package com.vivo.pulsar.example.admin;

import com.vivo.pulsar.example.utils.AdminClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

import java.util.Map;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-09-10 10:24
 */
public class BrokerTest {

    @Test
    public void testRuntimeConf() {
        PulsarAdmin pulsarAdmin = null;
        try {
            AdminClient adminClient = new AdminClient("local");
            pulsarAdmin = adminClient.getPulsarAdmin();
            System.out.println("pulsar admin create: " + pulsarAdmin);

            Map<String, String> configurations = pulsarAdmin.brokers().getRuntimeConfigurations();
            String brokerServicePort = StringUtils.substringBetween(configurations.get("brokerServicePort"), "[", "]");
            String webServicePort = StringUtils.substringBetween(configurations.get("webServicePort"), "[", "]");

            System.out.println("brokerServicePort: " + brokerServicePort);
            System.out.println("webServicePort: " + webServicePort);
        } catch (PulsarAdminException | PulsarClientException e) {
            e.printStackTrace();
        } finally {
            if (pulsarAdmin != null) {
                System.out.println("pulsar admin closed!");
                pulsarAdmin.close();
            }
            System.exit(0);
        }
    }
}
