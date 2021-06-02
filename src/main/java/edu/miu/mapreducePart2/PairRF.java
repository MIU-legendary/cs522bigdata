package edu.miu.mapreducePart2;

import edu.miu.mapreduce.Window;
import edu.miu.utils.HadoopUtils;
import edu.miu.mapreduce.WordCount;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.File;
import java.io.IOException;
import java.util.List;

public class PairRF extends Configured implements Tool
{

    public static class PairRFMapper extends Mapper<LongWritable, Text, Pair, IntWritable>
    {

        private final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<Window> windowList = edu.miu.utils.FileUtils.extractWindowFromString(value.toString());

            for(Window window: windowList){
                for(String v: window.getValues()){
                    Pair pair = new Pair();
                    pair.setKey(new Text(window.getKey()));
                    pair.setValue(new Text(v));
                    context.write(pair, ONE);
                }
            }

        }
    }

    public static class PairRFReducer extends Reducer<Pair, IntWritable, Text, IntWritable>
    {
        private DoubleWritable totalCount = new DoubleWritable();
        private Text currentWord = new Text("");
        final private Text CONST_STAR_CHAR = new Text("*");

        @Override
        public void reduce(Pair pair, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            Text pairText = new Text(pair.toString());
            context.write(pairText, new IntWritable(sum));
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