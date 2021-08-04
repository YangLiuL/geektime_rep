package myRpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyRpcClient {
    public static void main(String[] args){
        try {
            IMyRpcInterface proxy = (IMyRpcInterface) RPC.getProxy(IMyRpcInterface.class, 1L,
                    new InetSocketAddress("127.0.0.1", 8989)
                    , new Configuration());
            System.out.println(proxy.findName("G20210735010031"));
            System.out.println(proxy.findName("20210123456789"));
            System.out.println(proxy.findName("askdjlkasjdoasjo"));
        }catch (IOException exception){
            exception.printStackTrace();
        }
    }
}
