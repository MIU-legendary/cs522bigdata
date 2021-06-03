package edu.miu.mapreducePart3;

import org.apache.hadoop.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class MyMapWritable extends MapWritable{
    public int getTotal() {
        int sum = 0;
        Set<Writable> keySet = this.keySet();
        for (Object key : keySet) {
            int vl = Integer.valueOf(this.get(key).toString());
            sum += vl;
        }
        return sum;
    }

    public void generateFrequently(int total) {
        Set<Writable> keySet = this.keySet();

        for (Writable key : keySet) {
            int vl = Integer.parseInt(this.get(key).toString());
            double fre = (double) vl / total;

            DoubleWritable resultFre = new DoubleWritable();
            resultFre.set(fre);
            this.put(key, resultFre);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        result.append("[");
        for (Object key : keySet) {
            result.append("{" + key.toString() + " , " + this.get(key) + "}");
        }
        result.append("]");
        return result.toString();
    }

    public void add(MyMapWritable from) {
        for (Writable elementKey : from.keySet()) {
            if (containsKey(elementKey)) {
//                if(elementKey.toString().equals("D76")) {
//                    System.out.println("hehe");
//                }
                int old = ((IntWritable) get(elementKey)).get() ;
                int addMore = ((IntWritable) from.get(elementKey)).get();
                put(elementKey, new IntWritable(old + addMore));

            } else {
                put(elementKey, from.get(elementKey));
            }
        }
    }
}