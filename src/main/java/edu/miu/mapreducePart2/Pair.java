package edu.miu.mapreducePart2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements WritableComparable<Pair> {

    private final Text key;
    private final Text value;


    public Pair(Text key, Text value) {
        this.key = key;
        this.value = value;
    }


    public Pair(String key, String value) {
        this(new Text(key), new Text(value));
    }

    public Pair() {
        this.key = new Text();
        this.value = new Text();
    }


    @Override
    public int compareTo(Pair o) {
        int keyCheck = this.key.compareTo(o.getKey());
        if (keyCheck != 0)
            return keyCheck;


        if (value.toString().equals("*")) {
            return -1;
        } else if (o.getValue().toString().equals("*")) {
            return 1;
        }
        return value.compareTo(o.getValue());
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
    public String toString() {
        return String.format("(%s,%s)", key, value);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;


        if (o == null)
            return false;


        if (!(o instanceof Pair))
            return false;


        Pair word = (Pair) o;


        if (value != null ? !value.equals(word.value) : word.value != null)
            return false;


        if (key != null ? !key.equals(word.key) : word.key != null)
            return false;


        return true;
    }


    @Override
    public int hashCode() {
        String keyString = key == null ? "" : key.toString();
        String valueString = value == null ? "" : value.toString();
        return new Text(keyString + valueString).hashCode();
    }

    public void setKey(String key) {
        this.key.set(key);
    }

    public void setValue(String value) {
        this.value.set(value);
    }

    public Text getKey() {
        return key;
    }

    public Text getValue() {
        return value;
    }
}
