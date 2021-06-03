package edu.miu.part3;

import org.apache.hadoop.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class CustomMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        // "[{},{}]"
        if(keySet().isEmpty()){
            return "[]";
        }
        result.append("[");
        entrySet().forEach(e->result.append("{").append(e.getKey()).append(",").append(e.getValue()).append("}").append(","));
        result.deleteCharAt(result.length()-1);
        result.append("]");
        return result.toString();
    }
}