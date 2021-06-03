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

    public void generateFrequently() {
        int rs = 0;
        int total = getTotal();
        Set<Writable> keySet = this.keySet();
        DoubleWritable resultFre = new DoubleWritable();
        for (Writable key : keySet) {
            int vl = Integer.valueOf(this.get(key).toString());
            double fre = (double) vl / total;
            resultFre.set(fre);
            this.put(key , resultFre);
        }
    }

    private int total = 0;

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