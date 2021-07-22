package com.vivo.pulsar.example.utils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author sikang.liu
 * @date 2021-03-19 11:21
 * @description
 */
public class AdminClient {

    private PulsarAdmin admin;

    public AdminClient() throws PulsarClientException {
        String url = "http://10.101.50.13:8084,10.101.50.14:8084,10.101.50.15:8084";
        String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";
        //pulsar-admin:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.OX2P8rYrvuX0AMf98pG8YzHdZKxHdhseni3ET_Y1WAk
        String authParams = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItYWRtaW4ifQ.OX2P8rYrvuX0AMf98pG8YzHdZKxHdhseni3ET_Y1WAk";

        admin = PulsarAdmin.builder()
                .authentication(authPluginClassName, authParams)
                .serviceHttpUrl(url)
                .build();
    }

    public PulsarAdmin getPulsarAdmin() {
        return admin;
    }

    public static void main(String[] args) throws Exception {
        AdminClient adminClient = new AdminClient();
        PulsarAdmin pulsarAdmin = adminClient.getPulsarAdmin();
        String namespace = "lsk_tenant/lsk_ns";
        String topic = "persistent://lsk_tenant/lsk_ns/topic3p";

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

        String namespace3 = "lsk_tenant/lsk_ns3";
        CompletableFuture<Void> namespaceAsync = pulsarAdmin.namespaces().createNamespaceAsync(namespace3, 5);
        Void aVoid = namespaceAsync.get();
        if (null == aVoid) System.out.println("名称空间创建成功！");

        System.exit(0);


    }

}
