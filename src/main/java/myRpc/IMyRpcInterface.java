package myRpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IMyRpcInterface extends VersionedProtocol {
    long versionID = 1L;
    //根据学号查找
    String findName(String studentId);
}
