package listart.hadoop.rpc.server;

import listart.hadoop.rpc.School;
import listart.hadoop.rpc.impl.SchoolImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class SchoolServer {

    public static void main(String[] args) {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(12345);
        builder.setProtocol(School.class);
        builder.setInstance(new SchoolImpl());

        try {
            RPC.Server server = builder.build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
