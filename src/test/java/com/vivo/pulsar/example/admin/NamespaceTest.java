package com.vivo.pulsar.example.admin;

import com.google.common.collect.Sets;
import com.vivo.pulsar.example.utils.AdminClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-08-21 9:56
 */
public class NamespaceTest {

    @Test
    public void namespace() {
        PulsarAdmin pulsarAdmin = null;
        try {
            AdminClient adminClient = new AdminClient("local");
            pulsarAdmin = adminClient.getPulsarAdmin();
            System.out.println("pulsar admin create: " + pulsarAdmin);

            String tenant = "kop-tn";
            String namespace = "kop-tn/kop-ns";

            pulsarAdmin.namespaces().createNamespace(namespace);

//            pulsarAdmin.namespaces().deleteNamespace(namespace, true);

            List<String> namespaces = pulsarAdmin.namespaces().getNamespaces(tenant);
            System.out.println(namespaces);

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


    @Test
    public void testGrantPermission() {
        PulsarAdmin pulsarAdmin = null;
        try {
            AdminClient adminClient = new AdminClient("local");
            pulsarAdmin = adminClient.getPulsarAdmin();
            System.out.println("pulsar admin create: " + pulsarAdmin);

            String namespace = "my-tenant/my-ns";
            String role = "kop_role1";

            Set<AuthAction> actions = Sets.newHashSet(AuthAction.produce, AuthAction.consume);
            pulsarAdmin.namespaces().grantPermissionOnNamespace(namespace, role, actions);

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

    public static void main(String[] args) {
        
    }
}


