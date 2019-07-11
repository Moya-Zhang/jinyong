import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.*;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class task5 {

    static HashMap<String, Integer> label = new HashMap<String, Integer>();
    static HashMap<String, Integer> preLabel = new HashMap<String, Integer>();
    static int time = 0;
    static int iterTime = 5;

    public static class LPAMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException {
            if (time == 0) {
                try {
                    Path path = new Path(context.getConfiguration().get("name_list", null));
                    FileReader fr = new FileReader(path.toString());
                    BufferedReader bf = new BufferedReader(fr);
                    String str;
                    int i = 0;
                    while ((str = bf.readLine()) != null) {
                        str = str.replace("\n", "");
                        preLabel.put(str, i);
                        i++;
                    }
                    bf.close();
                    fr.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            else {
                preLabel = label;
                label.clear();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] itr = value.toString().split("\t");
            String curr = itr[0];
            String[] neighbors = itr[1].split("\\|");
            HashMap<Integer, Double> labelWeights = new HashMap<Integer, Double>();
            for (int i = 0; i < neighbors.length; i++) {
                String[] neighbor = neighbors[i].split(",");
                String name = neighbor[0];
                double weight = Double.parseDouble(neighbor[1]);
                if (labelWeights.containsKey(preLabel.get(name))) {
                    double sum = labelWeights.get(preLabel.get(name));
                    sum += weight;
                    labelWeights.put(preLabel.get(name), sum);
                }
                else {
                    labelWeights.put(preLabel.get(name), weight);
                }
            }

            double max = -1;
            Integer l = -1;
            for (Integer k: labelWeights.keySet()) {
                if (labelWeights.get(k) > max) {
                    max = labelWeights.get(k);
                    l = k;
                }
            }


            label.put(curr, l);
            context.write(new Text(curr), new Text(l.toString()));
        }
    }

    public static class LPAReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            for (Text v : values)
                context.write(key, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: LPA of <in> <out>");
            System.exit(2);
        }
        conf.set("name_list","task2/people_name_list.txt");
        for (; time < iterTime; time++) {
            Job job = Job.getInstance(conf, "LPA");
            job.setJarByClass(task5.class);
            job.setMapperClass(LPAMapper.class);
            job.setReducerClass(LPAReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+ "/res" + time));

            job.waitForCompletion(true);
        }

    }
}
