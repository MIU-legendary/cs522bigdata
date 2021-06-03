package edu.miu.mapreduce;

import edu.miu.utils.HadoopUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InMappingWordCountAverage extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new InMappingWordCountAverage(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "WordCount");
        job.setJarByClass(InMappingWordCountAverage.class);

        job.setMapperClass(WordCountAverageMapper.class);
        job.setReducerClass(WordCountAverageReducer.class);
        job.setNumReduceTasks(2);

        job.setMapOutputValueClass(Pair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class WordCountAverageMapper extends Mapper<LongWritable, Text, Text, Pair> {
        private final Text word = new Text();
        private final DoubleWritable value = new DoubleWritable();
        private Map<String, Pair> H = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitArray = value.toString().split("\\s+");
            String quantity = splitArray[splitArray.length - 1];
            String hostname = splitArray[0];
            if(isNumeric(quantity)) {
                if(H.get(hostname) == null){
                    Pair pair = new Pair(Double.parseDouble(quantity), 1);
                    H.put(hostname, pair);
                }else {
                    Pair pair = H.get(hostname);
                    pair.setAverage(pair.getAverage()+Double.parseDouble(quantity));
                    pair.setCount(pair.getCount() + 1);
                    H.put(hostname, pair);
                }
            }
//            IntWritable wQuantity = new IntWritable(Integer.parseInt(quantity));
//            word.set(hostname);
//            context.write(word, wQuantity);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> keySet = H.keySet();

            for(String key:keySet){
                Pair pair = H.get(key);
                word.set(key);
                context.write(word, pair);
            }
        }
    }

    public static class WordCountAverageReducer extends Reducer<Text, Pair, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (Pair val : values) {
                sum += val.getAverage();
                count += val.getCount();
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch(NumberFormatException e){
            return false;
        }
    }

    public static class Pair implements Writable {
        private Double average = (double) 0;
        private long count = 1;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(average);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            average = in.readDouble();
            count = in.readLong();
        }

        public Pair() {
        }

        public Pair(Double average, long count) {
            this.average = average;
            this.count = count;
        }

        public Double getAverage() {
            return average;
        }

        public void setAverage(Double average) {
            this.average = average;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}
