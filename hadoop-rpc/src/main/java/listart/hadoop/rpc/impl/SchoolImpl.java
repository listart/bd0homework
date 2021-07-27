package listart.hadoop.rpc.impl;

import listart.hadoop.rpc.School;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class SchoolImpl implements School {
    public static final Logger logger = Logger.getLogger(SchoolImpl.class);
    public static final Map<Long, String> studentRepo;

    static {
        Map<Long, String> repo = new HashMap<>();
        repo.put(20200579010082L, "Listart");
        repo.put(20210123456789L, "心心");

        studentRepo = repo;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) {
        System.out.println("SchoolImpl.getProtocolVersion protocol = " + protocol + "clientVersion = " + clientVersion);
        return School.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) {
        long serverVersion = getProtocolVersion(protocol, clientVersion);

        return ProtocolSignature.getProtocolSignature(clientMethodsHash, serverVersion, School.class);
    }

    @Override
    public String findName(long studentId) {
        logger.info("SchoolImpl.findName studentId = " + studentId);

        if (studentRepo.containsKey(studentId))
            return studentRepo.get(studentId);

        return null;
    }
}
