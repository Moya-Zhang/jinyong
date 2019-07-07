import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class task2 {

    public static class cooccurrenceMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String temp;
            Set<String> words = new HashSet<String>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                temp = itr.nextToken();
                words.add(temp);
            }
            for (String word : words) {
                for (String word2 : words) {
                    if (!word.equals(word2)) {
                        Text keyword = new Text();
                        keyword.set(word + "," + word2);
                        context.write(keyword, new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class SumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class sumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();

            }
            IntWritable result=new IntWritable();
            result.set(sum);
            context.write( key,result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: co-occurrence statistics of <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "co-occurrence statistics");
        job.setJarByClass(task2.class);
        job.setMapperClass(cooccurrenceMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(sumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
