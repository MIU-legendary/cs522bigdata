package edu.miu.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class Pair implements WritableComparable<Pair> {
    private Text sum;
    private Text count;


    public Pair(Text sum, Text count) {
        this.sum = sum;
        this.count = count;
    }


    public Pair(String sum, String count){
        this(new Text(sum), new Text(count));
    }


    public Pair() {
        this.sum = new Text();
        this.count = new Text();
    }


    @Override
    public int compareTo(Pair o) {
        int sumCheck = this.sum.compareTo(o.getsum());
        if (sumCheck != 0)
            return sumCheck;


        if (count.toString().equals("*")) {
            return -1;
        }else if (o.getcount().toString().equals("*")) {
            return 1;
        }
        return count.compareTo(o.getcount());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sum.write(out);
        count.write(out);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        sum.readFields(in);
        count.readFields(in);
    }


    @Override
    public String toString() {
        return String.format("(%s,%s)", sum, count);
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


        if (!Objects.equals(count, word.count))
            return false;


        return Objects.equals(sum, word.sum);
    }


    @Override
    public int hashCode() {
        return new Text(sum == null ? "" : sum.toString() + count == null ? "" : count.toString()).hashCode();
    }


    public void setsum(String sum) {
        this.sum.set(sum);
    }


    public void setcount(String count) {
        this.count.set(count);
    }


    public Text getsum() {
        return sum;
    }


    public Text getcount() {
        return count;
    }
}
