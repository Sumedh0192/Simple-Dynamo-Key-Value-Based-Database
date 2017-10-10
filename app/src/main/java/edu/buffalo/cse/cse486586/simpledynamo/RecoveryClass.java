package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by sumedh on 5/9/17.
 */

public class RecoveryClass implements Serializable{

    public Boolean isRecoveryRequest;
    public Boolean isReplicaRecovery;
    public Boolean isFirstSuccessor;
    public Boolean isFirstPredeccessor;
    public String secondPredeccessor;
    public String firstPredeccessor;
    public Map<String, String> recoveryDataMap;
    public Map<String, String> backupDataMap;
    public Map<String, Integer> versioningMap;
    public String portnum;

}
