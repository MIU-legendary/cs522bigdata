package edu.miu.mapreducePart3;

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

public class StripeApproach extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new StripeApproach(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "StripeApproach");
        job.setJarByClass(StripeApproachMapper.class);

        job.setMapperClass(StripeApproachMapper.class);
        job.setReducerClass(StripeApproachReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class StripeApproachMapper extends Mapper<LongWritable, Text, Text, MyMapWritable> {
        private Map<String, MyMapWritable> occurrenceMap = new HashMap<String, MyMapWritable>();
        private Map<String, Integer> totalMap = new HashMap<String, Integer>();
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<Window> windowList = edu.miu.utils.FileUtils.extractWindowFromString(value.toString());

            windowList.forEach(x -> {
                System.out.println("loop " + x.toString());
            });

            for (Window window : windowList) {
                MyMapWritable childMap = new MyMapWritable();

                word.set(window.getKey());
                String keyName = window.getKey();

                for (String v : window.getValues()) {
                    Text neighbor = new Text(v);
                    childMap.put(neighbor, new IntWritable(1));
                }

                if (occurrenceMap.containsKey(keyName)) {
//                    if(keyName.equals("C31")) {
//                        System.out.println("ok come");
//                    }
                    if(childMap.size() > 0) {
                        occurrenceMap.get(keyName).add(childMap);
                    }

                } else {
                    occurrenceMap.put(keyName, childMap);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            occurrenceMap.forEach((key, childMap) -> {
                try {
                    word.set(key);
                    context.write(word, childMap);

                    System.out.println("tungtt:  key " + key.toString() + " map " + childMap.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static class StripeApproachReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
        @Override
        public void reduce(Text key, Iterable<MyMapWritable> values, Context context)
                throws IOException, InterruptedException {
            MyMapWritable reduceMap = new MyMapWritable();
            for (MyMapWritable listPair : values) {
                listPair.generateFrequently();
                reduceMap.add(listPair);
            }
            context.write(key, reduceMap);
        }
    }

}
