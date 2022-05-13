package com.vivo.pulsar.example.admin;

import com.google.common.collect.Sets;
import com.vivo.pulsar.example.utils.AdminClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
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
public class TenantTest {


    @Test
    public void tenant() {
        PulsarAdmin pulsarAdmin = null;
        try {
            AdminClient adminClient = new AdminClient("local");
            pulsarAdmin = adminClient.getPulsarAdmin();
            System.out.println("pulsar admin create: " + pulsarAdmin);

            String tenant = "kop-tn";

            TenantInfoImpl tenantInfo = TenantInfoImpl.builder()
                    .allowedClusters(Sets.newHashSet("standalone"))
                    .build();
            pulsarAdmin.tenants().createTenant(tenant, tenantInfo);

            List<String> tenants = pulsarAdmin.tenants().getTenants();
            System.out.println(tenants);

            TenantInfo tenant1 = pulsarAdmin.tenants().getTenantInfo(tenant);
            System.out.println(tenant1.toString());

            //pulsarAdmin.tenants().deleteTenant(tenant);

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
