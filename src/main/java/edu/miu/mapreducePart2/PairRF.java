package edu.miu.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class PairRF extends Configured implements Tool
{

    public static class PairRFMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        private DoubleWritable ONE = new DoubleWritable(1);
        private DoubleWritable totalCount = new DoubleWritable();
        private int count = 0;
        final private Text CONST_STAR_CHAR = new Text("*");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");
            if (words.length > 1) {
                for (int i = 0; i < words.length - 1; i++) {
                    count = 0;
                    String termU = words[i];
                    for (int j = i + 1; j < words.length; j++) {
                        String termV = words[j];
                        if (termU.equalsIgnoreCase(termV))
                            break;
                        count++;
                        context.write(new Pair(termU, termV), ONE);
                    }
                    totalCount.set((double) count);
                    context.write(new Pair(words[i], "*"), totalCount);
                }
            }
        }
    }

    public static class PairRFReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable>
    {
        private DoubleWritable totalCount = new DoubleWritable();
        private Text currentWord = new Text("");
        final private Text CONST_STAR_CHAR = new Text("*");

        @Override
        protected void reduce(Pair key, Iterable<DoubleWritable> counts, Context context)
                throws IOException, InterruptedException {
            if (key.getcount().equals(CONST_STAR_CHAR)) {
                if (key.getsum().equals(currentWord)) {
                    totalCount.set(totalCount.get() + getCountSum(counts));
                } else {
                    currentWord.set(key.getsum());
                    totalCount.set(0);
                    totalCount.set(getCountSum(counts));
                }
            } else {
                int count = getCountSum(counts);
                context.write(key, new DoubleWritable((double) count / totalCount.get()));
            }
        }

        private int getCountSum(Iterable<DoubleWritable> counts) {
            int total = 0;
            for (DoubleWritable count : counts) {
                total += count.get();
            }
            return total;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new WordCount(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {


        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "PairRF");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(PairRFMapper.class);
        job.setReducerClass(PairRFReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
