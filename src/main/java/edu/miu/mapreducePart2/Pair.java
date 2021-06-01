package edu.miu.mapreducePart2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.glassfish.grizzly.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Pair implements WritableComparable<Pair> {
    Text key;
    Text value;

    public Pair(Text key, Text value) {
        this.key = key;
        this.value = value;
    }

    public Pair() {

    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public void setValue(Text value){
        this.value = value;
    }

    public Text getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "key=" + key +
                ", value=" + value +
                "} \r\n";
    }


    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public int compareTo(Pair P) {
        int keyCompare = this.key.compareTo(P.getKey());
        int valueCompare = this.value.compareTo(P.getValue());

        if(keyCompare == 0){
            if(valueCompare < 0) return -1;
            else if (valueCompare > 0) return 1;
            return 0;
        }

        return keyCompare;
    }
}
