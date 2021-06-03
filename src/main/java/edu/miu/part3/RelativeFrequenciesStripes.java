package edu.miu.part3;

import edu.miu.mapreduce.Window;
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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
public class RelativeFrequenciesStripes extends Configured implements Tool {

    public static class Utils {
        private Utils() {
        }
        public static List<String> window(String[] tokens, int i) {
            List<String> result = new ArrayList<>();
            if (i == tokens.length - 1) return result;
            String start = tokens[i];
            int j = i + 1;
            while (j < tokens.length && !start.equals(tokens[j])) {
                result.add(tokens[j]);
                j++;
            }
            return result;
        }

        public static int getInt(Writable writable) {
            return Integer.parseInt(writable.toString());
        }
    }

    public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                word.set(token);
                MapWritable h = new MapWritable();
                Utils.window(tokens, i).forEach(e -> {
                    Text keyV = new Text(e);
                    if (h.get(keyV) != null) {
                        int result = Utils.getInt(h.get(keyV)) + 1;
                        h.put(keyV, new IntWritable(result));
                    } else {
                        h.put(keyV, one);
                    }
                });

                context.write(word, h);
            }
        }


    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, CustomMapWritable> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            CustomMapWritable hf = new CustomMapWritable();
            for (MapWritable val : values) {
                for (Writable keyW : val.keySet()) {
                    int currentValue = Utils.getInt(val.get(keyW));
                    if (hf.get(keyW) != null) {
                        int updatedValue = Utils.getInt(hf.get(keyW));
                        hf.put(keyW, new IntWritable(currentValue + updatedValue));
                    } else {
                        hf.put(keyW, val.get(keyW));
                    }
                    sum += Utils.getInt(val.get(keyW));
                }
            }


            int finalSum = sum;
            hf.replaceAll((k, v)-> {
                double d = (double)((IntWritable)v).get() / finalSum;
                return new DoubleWritable(d);
            });
            context.write(key, hf);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new RelativeFrequenciesStripes(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));

        Job job = new Job(getConf(), "RelativeFrequenciesStripes");
        job.setJarByClass(RelativeFrequenciesStripes.class);

        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);
//		job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}