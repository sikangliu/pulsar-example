package com.vivo.pulsar.example.admin;

import com.vivo.pulsar.example.utils.AdminClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-01 17:13
 */
public class TopicTest {

    @Test
    public void topic() throws Exception {
        PulsarAdmin pulsarAdmin = null;
        try {
            AdminClient adminClient = new AdminClient("local");
            pulsarAdmin = adminClient.getPulsarAdmin();
            System.out.println("pulsar admin create: " + pulsarAdmin);

            //persistent://kop-tn/kop-ns/kop-test
            String namespace = "kop-tn/kop-ns";
            String topic = "kop-tn/kop-ns/kop-test";
            Integer partition = 2;
            String sub = "kop-sub";

//            pulsarAdmin.topics().deletePartitionedTopic(topic);
            List<String> topicList = pulsarAdmin.topics().getPartitionedTopicList(namespace);
            if (!topicList.contains("persistent://" + topic)) {
                pulsarAdmin.topics().createPartitionedTopic(topic, partition);
                Thread.sleep(3000L);
            } else {
                System.out.println(topicList);
            }

//            int RateInMsg = -1;
//            long RateInByte = 50;
//            PublishRate publishRate2 = new PublishRate(RateInMsg, RateInByte);
//            pulsarAdmin.topics().setPublishRate(topic, publishRate2);

//            pulsarAdmin.topics().createSubscription(topic, sub, MessageId.earliest);

//            PublishRate publishRate3 = pulsarAdmin.topics().getPublishRate(topic);
//            System.out.println(publishRate3.toString());

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
