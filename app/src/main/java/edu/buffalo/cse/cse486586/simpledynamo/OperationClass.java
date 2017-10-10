package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by sumedh on 4/12/17.
 */

public class OperationClass implements Serializable {

    public String type;
    public String selection;
    public String key;
    public String value;
    public String parentPort;

    public OperationClass(String type, String selection, String key, String value, String parentPort){
        this.type = type;
        this.selection = selection;
        this.key = key;
        this.value = value;
        this.parentPort = parentPort;
    }

}
