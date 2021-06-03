package edu.miu.mapreduce;

import edu.miu.utils.HadoopUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InMapperWordCount extends Configured implements Tool {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Pair> {
        private Text word = new Text();
        private Map<String, List<Integer>> H = new HashMap<>();
        private Pair pair = new Pair();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] splitArray = value.toString().split("\\s+");
                if (!splitArray[splitArray.length - 1].equals("-")) {
                    List<Integer> listQuantity = H.getOrDefault(splitArray[0], new ArrayList<>());
                    listQuantity.add(Integer.valueOf(splitArray[splitArray.length - 1]));
                    H.put(splitArray[0], listQuantity);
                }

            } catch (Exception $e) {
                $e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (String key : H.keySet()) {
                word.set(key);
                int count = H.get(key).size();
                int sum = H.get(key).stream().mapToInt(value -> value).sum();
                pair.setAverage((double) (sum / count));
                pair.setCount(count);
                context.write(word, pair);
            }
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

    public static class WordCountReducer extends Reducer<Text, Pair, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (Pair val : values) {
                sum += val.getAverage() * val.getCount();
                count += val.getCount();
            }
            result.set((double) sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new InMapperWordCount(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {


        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "InMapperWordCount");
        job.setJarByClass(InMapperWordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Pair.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
