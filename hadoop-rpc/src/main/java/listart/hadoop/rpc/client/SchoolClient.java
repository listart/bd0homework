package listart.hadoop.rpc.client;

import listart.hadoop.rpc.School;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SchoolClient {
    public static void main(String[] args) {
        try {
            School proxy = RPC.getProxy(
                    School.class,
                    1L,
                    new InetSocketAddress("127.0.0.1", 12345),
                    new Configuration()
            );

            System.out.println("proxy.findName(20200579010082L) = " + proxy.findName(20200579010082L));
            System.out.println("proxy.findName(20210123456789L) = " + proxy.findName(20210123456789L));
            System.out.println("proxy.findName(20210000000000L) = " + proxy.findName(20210000000000L));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
