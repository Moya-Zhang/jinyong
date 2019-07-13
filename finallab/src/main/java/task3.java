import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class task3 {

    public static class NormalizationMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] itr = value.toString().split("\t");
            Text word = new Text();
            word.set(itr[0]);
            context.write(word, new IntWritable(Integer.parseInt(itr[1])));
        }
    }

    public static class NewPartitioner
            extends HashPartitioner<Text, Object> {

        @Override
        public int getPartition(Text key, Object value, int numReduceTasks) {
            String term;
            term = key.toString().split(",")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class NormalizationReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp;
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split(",")[0]);
            temp = key.toString().split(",")[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            word2.set(temp + "," + sum + "|");
            if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
                StringBuilder out = new StringBuilder();
                long count = 0;
                for (String p : postingList) {
                    //out.append(";");
                    count += Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf("|")));
                }
//                out.append("[");
                for(String p:postingList){
                    out.append(p.substring(0,p.indexOf(",")+1));
                    Long pCount = Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf("|")));
                    double pWeight = (double)pCount/(double)count;
                    pWeight = (double) Math.round(pWeight * 100000) / 100000;
                    out.append(pWeight);
                    if(postingList.indexOf(p)!=postingList.size()-1)
                        out.append("|");
//                    else
//                        out.append("]");
                }
                // out.append("<total," + count + ">.");
                if (count > 0 && postingList.size() > 0) {
                    context.write(CurrentItem, new Text(out.toString()));
                }
                postingList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString());
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            long count = 0;
            for (String p : postingList) {

                //out.append(";");
                count += Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf("|")));
            }
            for(String p:postingList){
                out.append(p.substring(0,p.indexOf(",")+1));
                Long pCount = Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf("|")));
                double pWeight = (double)pCount/(double)count;
                pWeight = (double) Math.round(pWeight * 100000) / 100000;
                out.append(pWeight);
                if(postingList.indexOf(p)!=postingList.size()-1)
                    out.append("|");
//                else
//                    out.append("]");
            }
            // out.append("<total," + count + ">.");
            if (count > 0 && postingList.size() > 0) {
                context.write(CurrentItem, new Text(out.toString()));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: InvertedIndex <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(task3.class);
        job.setMapperClass(NormalizationMapper.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(NormalizationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
