package com.vivo.pulsar.example.utils;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.springframework.util.CollectionUtils;

import javax.swing.text.StyledEditorKit;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author sikang.liu
 * @date 2021-03-30 15:51
 * @description
 */
public class BkAdminClient {


    public static void main(String[] args) {
        BookKeeperAdmin bkadmin = null;
        try {
            String zk = "10.101.129.84:2181,10.101.129.85:2181,10.101.129.86:2181/pulsar_dev";
            String zkServers = Stream.of(zk.split(",")).reduce((first, last) -> last).get();
            ClientConfiguration clientConf = new ClientConfiguration();
            clientConf.setMetadataServiceUri("zk+null://" + zkServers + "/ledgers");
            clientConf.setZkTimeout(60000);
            //bkadmin = new BookKeeperAdmin("10.101.129.86:2181/pulsar_dev");
            bkadmin = new BookKeeperAdmin(clientConf);

            Collection<BookieId> allBookies = bkadmin.getAllBookies();
            System.out.println("get allBookies:" + allBookies);

            for (BookieId bookie : allBookies) {
                System.out.println("bookie info :" + bookie.getId());
                if ("10.101.130.47:3181".equals(bookie.getId())) {
                    System.out.println("bookie service info:" + bkadmin.getBookieServiceInfo(bookie.getId()));
                    BookieServiceInfo bookieServiceInfo = bkadmin.getBookieServiceInfo(bookie.getId());
                    List<BookieServiceInfo.Endpoint> endpoints = bookieServiceInfo.getEndpoints();
                    System.out.println("endpoints(0):" + endpoints.get(0).getPort());
                    for (BookieServiceInfo.Endpoint endpoint : endpoints) {
                        System.out.println("bookie ip and port info:" + endpoint.getHost() + ":" + endpoint.getPort());
                    }

                }
            }

            Set<BookieId> bookies = new HashSet<>(allBookies.size());
            for (BookieId bookieId : allBookies) {
                bookies.add(bookieId);
            }
            SortedMap<Long, LedgerMetadata> sortedMap = bkadmin.getLedgersContainBookies(bookies);
            System.out.println("sortedMap:" + sortedMap);


            Collection<BookieId> availableBookies = bkadmin.getAvailableBookies();
            System.out.println("get availableBookies:" + availableBookies);

//        Iterable<Long> longs = bkadmin.listLedgers();
//        Iterator<Long> iterator = longs.iterator();
//        while (iterator.hasNext()){
//            Long aLong = iterator.next();
//            System.out.println("get longs:"+aLong);
//        }
//

            ClientConfiguration conf = bkadmin.getConf();
            Iterator<String> keys = conf.getKeys();
            while (keys.hasNext()) {
                String next = keys.next();
                System.out.println("get conf:" + next);
            }
            System.out.println("getBookieInfoTimeout:" + conf.getString("getBookieInfoTimeout"));
            System.out.println("getMinNumRacksPerWriteQuorum:" + conf.getMinNumRacksPerWriteQuorum());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        } finally {
            try {
                bkadmin.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BKException e) {
                e.printStackTrace();
            } finally {
                System.exit(0);
            }
        }
    }

}
