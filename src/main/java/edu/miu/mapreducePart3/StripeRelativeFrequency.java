package edu.miu.mapreducePart3;

import edu.miu.mapreduce.Window;
import edu.miu.utils.HadoopUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StripeRelativeFrequency extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new StripeRelativeFrequency(), args);
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
        private final Text word = new Text();
        private final Map<String, MyMapWritable> G = new HashMap<String, MyMapWritable>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<Window> windowList = edu.miu.utils.FileUtils.extractWindowFromString(value.toString());

            for (Window window : windowList) {
                MyMapWritable H = new MyMapWritable();

                word.set(window.getKey());
                String keyName = window.getKey();

                for (String v : window.getValues()) {
                    Text neighbor = new Text(v);
                    IntWritable writeable = (IntWritable) H.getOrDefault(neighbor, new IntWritable(0));
                    H.put(neighbor, new IntWritable(writeable.get() + 1));
                }

                if (G.containsKey(keyName)) {
                    if (H.size() > 0) {
                        G.get(keyName).add(H);
                    }
                } else {
                    G.put(keyName, H);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            G.forEach((key, childMap) -> {
                try {
                    word.set(key);
                    context.write(word, childMap);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static class StripeApproachReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
        @Override
        public void reduce(Text key, Iterable<MyMapWritable> values, Context context)
                throws IOException, InterruptedException {
            MyMapWritable Hf = new MyMapWritable();

            int total = 0;
            List<MyMapWritable> myList = new ArrayList<>();
            for (MyMapWritable listPair : values) {
                total += listPair.getTotal();
                myList.add(listPair);
            }

            for (MyMapWritable listPair : myList) {
                listPair.generateFrequently(total);
                Hf.add(listPair);
            }
            context.write(key, Hf);
        }
    }
}
