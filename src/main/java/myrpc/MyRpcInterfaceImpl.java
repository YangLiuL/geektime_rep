package myrpc;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;


import java.io.IOException;

public class MyRpcInterfaceImpl implements IMyRpcInterface{
    @Override
    public String findName(String studentId) {
        if(studentId.equals("20210123456789"))
        {
            return "心心";
        }else if(studentId.equals("G20210735010031")){
            return "刘洋";
        }else {
            return null;
        }
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return IMyRpcInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }

    public static void main(String[] args)
    {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(8989);
        builder.setProtocol(MyRpcInterfaceImpl.class);
        builder.setInstance(new MyRpcInterfaceImpl());

        try{
            Server server = builder.build();
            server.start();
        }
        catch (IOException exception){
            exception.printStackTrace();
        }
    }
}
