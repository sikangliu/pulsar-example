package com.vivo.pulsar.example.utils;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;

import javax.swing.text.StyledEditorKit;
import java.util.*;

/**
 * @author sikang.liu
 * @date 2021-03-30 15:51
 * @description
 */
public class BkAdminClient {


    public static void main(String[] args) throws Exception {
        BookKeeperAdmin bkadmin = new BookKeeperAdmin("10.101.50.13:2184/tianji");

        Collection<BookieId> allBookies = bkadmin.getAllBookies();
        System.out.println("get allBookies:"+allBookies);

        Set<BookieId> bookies = new HashSet<>(allBookies.size());
        for (BookieId bookieId : allBookies) {
            bookies.add(bookieId);
        }
        SortedMap<Long, LedgerMetadata> sortedMap = bkadmin.getLedgersContainBookies(bookies);
        System.out.println("sortedMap:"+sortedMap);


        Collection<BookieId> availableBookies = bkadmin.getAvailableBookies();
        System.out.println("get availableBookies:"+availableBookies);

        Iterable<Long> longs = bkadmin.listLedgers();
        Iterator<Long> iterator = longs.iterator();
        while (iterator.hasNext()){
            Long aLong = iterator.next();
            System.out.println("get longs:"+aLong);
        }

        ClientConfiguration conf = bkadmin.getConf();
        System.out.println("get conf:"+conf);



        bkadmin.close();

    }

}
