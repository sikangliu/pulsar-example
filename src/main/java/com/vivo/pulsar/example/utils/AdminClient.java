package com.vivo.pulsar.example.utils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.*;

import javax.sound.midi.Soundbank;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author sikang.liu
 * @date 2021-03-19 11:21
 * @description
 */
public class AdminClient {

    private PulsarAdmin admin;

    public AdminClient(String env) throws PulsarClientException {
        String url;
        String authParams;
        String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";
        if (env.equals("local")) {
            url = "http://localhost:8080";
            authParams = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.AV_jbPsAXb3rnhDm8-zOVt_Zwi_3jX-XNXMw_MU6WNU";
        } else {
            url = "http://10.101.129.85:8080";
            authParams = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXJhZG1pbiJ9.zSPFdsq2cA1KAveDf5AHGMp_ts8yWcrZORtl4_8J_NM";
        }
        admin = PulsarAdmin.builder()
                .authentication(authPluginClassName, authParams)
                .serviceHttpUrl(url)
                .build();
    }

    public PulsarAdmin getPulsarAdmin() {
        return admin;
    }

    public static void main(String[] args) throws Exception {
        AdminClient adminClient = new AdminClient("local");
        PulsarAdmin pulsarAdmin = adminClient.getPulsarAdmin();
        String namespace = "lsk_tenant/lsk_ns";
        String topic = "persistent://lsk_tenant/lsk_ns/topic3p";

//        List<String> activeBrokers = pulsarAdmin.brokers().getActiveBrokers("tianji");
//        System.out.println("active broker :" + activeBrokers.toString());
//
//        InternalConfigurationData internalConfigure = pulsarAdmin.brokers().getInternalConfigurationData();
//        String stateStorageServiceUrl = internalConfigure.getStateStorageServiceUrl();
//        System.out.println("stateStorageServiceUrl:" + stateStorageServiceUrl);
//        Map<String, String> map = pulsarAdmin.brokers().getRuntimeConfigurations();
//        System.out.println("动态配置：");
//        map.forEach((key, value) -> {
//            if ("brokerServicePort".equals(key) || "webServicePort".equals(key)) {
//                System.out.println(key + ":" + value);
//                System.out.println(value);
//            }
//        });
//        Set<AuthAction> actions = new HashSet<>();
//        actions.add(AuthAction.produce);
//        actions.add(AuthAction.consume);
//
//        String subject = "ns_admin";
//        CompletableFuture<Void> completableFuture = pulsarAdmin.namespaces().grantPermissionOnNamespaceAsync(namespace, subject, actions);
//        Void aVoid = completableFuture.get();
//        if (null == aVoid) System.out.println("名称空间授权成功");

//        String role = "lsk";
//        Set<AuthAction> actions  = Sets.newHashSet(AuthAction.produce, AuthAction.consume);
//        pulsarAdmin.topics().grantPermission(topic, role, actions);

        //获取名称空间下topic列表
        List<String> list = pulsarAdmin.topics().getList(namespace);
        System.out.println(list.toString());

//        String namespace = "lsk_tenant/lsk_ns";
//        CompletableFuture<Void> completableFuture = pulsarAdmin.namespaces().setDeduplicationStatusAsync(namespace, true);
//        Void aVoid = completableFuture.get();
//        if (null == aVoid) System.out.println("名称空间设置去重成功！");

//        CompletableFuture<Integer> snapshotIntervalAsync = pulsarAdmin.namespaces().getDeduplicationSnapshotIntervalAsync(namespace);
//        Integer integer = snapshotIntervalAsync.get();
//        System.out.println("snapshotIntervalAsync is:"+integer);

        String namespace3 = "lsk_tenant/lsk_ns10";
        Integer bundle = 20;
        //pulsarAdmin.namespaces().createNamespace(namespace3, bundle);
//        pulsarAdmin.namespaces().setNamespaceMessageTTL(namespace3, 3 * 60 * 60);
//        public RetentionPolicies(int retentionTimeInMinutes, int retentionSizeInMB) {
        RetentionPolicies retention = new RetentionPolicies(20, 1024);
        pulsarAdmin.namespaces().setRetention(namespace3, retention);
//        PersistencePolicies persistence = new PersistencePolicies(3, 3, 2, 0.0D);
//        pulsarAdmin.namespaces().setPersistence(namespace3, persistence);


        Policies policies = pulsarAdmin.namespaces().getPolicies(namespace3);
        System.out.println(policies);
//        CompletableFuture<Void> namespaceAsync = pulsarAdmin.namespaces().createNamespaceAsync(namespace3, 5);
//        Void aVoid = namespaceAsync.get();
//        if (null == aVoid) System.out.println("名称空间创建成功！");

        System.exit(0);


    }

}
