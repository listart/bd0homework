package listart.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface School extends VersionedProtocol {
    long versionID = 1L;

    String findName(long studentId);
}
