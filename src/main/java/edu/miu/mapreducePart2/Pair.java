package edu.miu.mapreducePart2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
                "key=" + key.toString() +
                ", value=" + value.toString() +
                "}";
    }


    @Override
    public void write(DataOutput out) throws IOException {
        this.key.write(out);
        this.value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.key.readFields(in);
        this.value.readFields(in);
    }

    @Override
    public int compareTo(Pair P) {
        int keyCompare = this.key.compareTo(P.getKey());
        int valueCompare = this.value.compareTo(P.getValue());

        if (keyCompare == 0) {
            return valueCompare;
        }
        return keyCompare;
    }
}
