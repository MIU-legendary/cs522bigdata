package edu.miu.part4;

import edu.miu.mapreduce.Window;
import edu.miu.mapreducePart2.Pair;
import edu.miu.mapreducePart3.MyMapWritable;
import edu.miu.utils.HadoopUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PairMapperStripeReducer extends Configured implements Tool {

    public static class PairRFMapper extends Mapper<LongWritable, Text, Pair, DoubleWritable> {

        private Map<Pair, Double> hashMap = new HashMap<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<Window> windowList = edu.miu.utils.FileUtils.extractWindowFromString(value.toString());
            for (Window window : windowList) {
                for (String v : window.getValues()) {
                    if (v == null || v.length() == 0) continue;
                    Pair pair = new Pair(window.getKey(), v);
                    hashMap.merge(pair, (double) 1, Double::sum);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Pair> keySet = hashMap.keySet();
            for (Pair p : keySet) {
                context.write(p, new DoubleWritable(hashMap.get(p)));
            }
        }
    }

    public static class PairRFReducer extends Reducer<Pair, DoubleWritable, Text, MyMapWritable> {
        private String wPrev = null;
        private Map<String, DoubleWritable> map = new HashMap<>();
        private final Text word = new Text();

        @Override
        public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            if (!pair.getKey().toString().equals(wPrev) && wPrev != null) {
                emitData(context);
            }
            double total = 0;
            for (DoubleWritable doubleWritable : values) {
                total += doubleWritable.get();
            }
            map.put(pair.getValue().toString(), new DoubleWritable(total));
            wPrev = pair.getKey().toString();
        }

        private void emitData(Context context) throws IOException, InterruptedException {
            double total = 0;
            for (String key : map.keySet()) {
                total += map.get(key).get();
            }
            MyMapWritable reduceMap = new MyMapWritable();
            for (String key : map.keySet()) {
                MyMapWritable listPair = new MyMapWritable();
                listPair.put(new Text(key), map.get(key));
                listPair.generateFrequently((int) total);
                reduceMap.add(listPair);
            }
            word.set(wPrev);
            context.write(word, reduceMap);
            map = new HashMap<>();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            emitData(context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new PairMapperStripeReducer(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "PairRF");
        job.setJarByClass(PairMapperStripeReducer.class);


        job.setMapperClass(PairRFMapper.class);
        job.setReducerClass(PairRFReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
