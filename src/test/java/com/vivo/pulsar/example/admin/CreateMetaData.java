package com.vivo.pulsar.example.admin;

import com.google.common.collect.Sets;
import com.vivo.pulsar.example.utils.AdminClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.junit.Test;

import java.util.List;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-01 16:09
 */
public class CreateMetaData {


    @Test
    public void createMetaData() {
        PulsarAdmin pulsarAdmin = null;
        try {
            AdminClient adminClient = new AdminClient("local");
            pulsarAdmin = adminClient.getPulsarAdmin();
            System.out.println("pulsar admin create: " + pulsarAdmin);

            String tenant = "kop";
            String namespace = "kop/kop-ns";
            String topic = "kop/kop-ns/kop-test";
            Integer partition = 2;
            String sub = "kop-sub";

            TenantInfoImpl tenantInfo = TenantInfoImpl.builder()
                    .allowedClusters(Sets.newHashSet("standalone"))
                    .build();
            pulsarAdmin.tenants().createTenant(tenant, tenantInfo);

            List<String> tenants = pulsarAdmin.tenants().getTenants();
            System.out.println(tenants);

            TenantInfo tenant1 = pulsarAdmin.tenants().getTenantInfo(tenant);
            System.out.println(tenant1.toString());

            pulsarAdmin.namespaces().createNamespace(namespace);

            List<String> namespaces = pulsarAdmin.namespaces().getNamespaces(tenant);
            System.out.println(namespaces);

            List<String> topicList = pulsarAdmin.topics().getPartitionedTopicList(namespace);
            if (!topicList.contains("persistent://" + topic)) {
                pulsarAdmin.topics().createPartitionedTopic(topic, partition);
                Thread.sleep(3000L);
            } else {
                System.out.println(topicList);
            }

            pulsarAdmin.topics().createSubscription(topic, sub, MessageId.earliest);

        } catch (PulsarAdminException | PulsarClientException | InterruptedException e) {
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
