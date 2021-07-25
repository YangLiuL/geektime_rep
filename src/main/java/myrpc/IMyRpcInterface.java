package myrpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IMyRpcInterface extends VersionedProtocol {
    long versionID = 1L;
    String findName(String studentId);
}
