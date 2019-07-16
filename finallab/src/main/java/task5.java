import java.io.*;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;


public class task5 {

//    HashMap<String, Integer> label = new HashMap<String, Integer>();
//    HashMap<String, Integer> preLabel = new HashMap<String, Integer>();
//    static int time = 0;
//    static int iterTime = 5;
//    FileSystem hdfs;
//
    public static class InitLabel {

        // static int i = 0;

        public static void run(String[] args)
                throws IOException,InterruptedException,ClassNotFoundException
        {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: InitLabel <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "initlabel");
            job.setJarByClass(InitLabel.class);
            job.setMapperClass(InitLabelMapper.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        }
        public static class InitLabelMapper
                extends Mapper<Object, Text, Text, Text>
        {
            public void map(Object key,Text value,Context context)
                    throws IOException,InterruptedException
            {
                // name+\t+label+\t+name1,weight1|name2,weight2......
                String s = value.toString();
                Text name = new Text(s.split("\t")[0]);
                String label = name.toString() + "\t";
                label += s.split("\t")[1];
                context.write(name, new Text(label));
            }
        }
    }

    public static class LPAIter {
        public static void run(String[] args)
                throws IOException,InterruptedException,ClassNotFoundException
        {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: LPAIter <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "LPAIter");
            job.setJarByClass(LPAIter.class);
            job.setMapperClass(LPAIterMapper.class);
            job.setReducerClass(LPAIterReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        }

        public static class LPAIterMapper
                extends Mapper<LongWritable,Text,Text,Text>
        {
            @Override
            public void map(LongWritable key,Text value,Context context)
                    throws IOException,InterruptedException
            {
                String line = value.toString();
                String[] tuple = line.split("\t");
                String name = tuple[0];
                String label = tuple[1];
                String[] neighbors = tuple[2].split("\\|");
                for(String neighbor : neighbors){
                    String[] tmp = neighbor.split(",");
                    //double weight = Double.parseDouble(tmp[1]);
                    context.write(new Text(tmp[0]), new Text(label + "#" + name));
                }
                //传递原始链接信息
                context.write(new Text(name), new Text("#" + tuple[2]));
                context.write(new Text(name), new Text("$" + label));

            }
        }

        public static class LPAIterReducer
                extends Reducer<Text,Text,Text,Text>
        {
            @Override
            public void reduce(Text key,Iterable<Text> values,Context context)
                    throws IOException,InterruptedException
            {
                String label = "";
                String neighbors = "";
                HashMap<String, String> nameLabel = new HashMap<>();
                for (Text text : values) {
                    String str = text.toString();
                    if (str.length() > 0 && str.charAt(0) == '$') {
                        label = str.replace("$", "");  // get label
                    } else if (str.length() > 0 && str.charAt(0) == '#') {
                        neighbors = str.replace("#", "");
                    } else if (str.length() > 0) {
                        String[] tmp = str.split("#");
                        nameLabel.put(tmp[1], tmp[0]);   // 邻居的<name,label>
                    }
                }

                HashMap<String, Double> labelWeight = new HashMap<>();
                String[] list = neighbors.split("\\|");
                for (String l : list) {
                    String name = l.split(",")[0];
                    String tag = nameLabel.get(name);
                    Double weight = Double.parseDouble(l.split(",")[1]);
                    if (labelWeight.containsKey(tag)) {
                        Double sum = labelWeight.get(tag);
                        sum += weight;
                        labelWeight.put(tag, sum);
                    } else {
                        labelWeight.put(tag, weight);
                    }
                }

                double max = -1;
                String maxLabel = "";
                for (String k: labelWeight.keySet()) {
                    if (labelWeight.get(k) > max) {
                        max = labelWeight.get(k);
                        maxLabel = k;
                    }
                }

                context.write(key, new Text(maxLabel + "\t" + neighbors));
            }
        }
    }

    public static class LPAViewer{

        public static void run(String[] args)
                throws IOException,InterruptedException,ClassNotFoundException
        {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: LPAViewer <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "LPAviewer");
            job.setJarByClass(LPAViewer.class);
            job.setMapperClass(LPAViewerMapper.class);
            job.setReducerClass(LPAViewerReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        }
        public static class LPAViewerMapper
                extends Mapper<Object,Text, Text,Text>
        {

            public void map(Object key,Text value,Context context)
                    throws IOException,InterruptedException
            {
                String[] line = value.toString().split("\t");
                String name = line[0];
                String label = line[1];
                // context.write(new Text(name),new Text(label));
                context.write(new Text(label), new Text(name));
            }
        }

        public static class LPAViewerReducer
                extends Reducer<Text, Text, Text, Text>
        {
            public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException, InterruptedException
            {
                StringBuilder sb = new StringBuilder();
                for (Text text : values) {
                    sb.append(text.toString());
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length()-1);
                context.write(key, new Text(sb.toString()));
            }
        }
    }


//    public class LPAMapper extends Mapper<Object, Text, Text, Text> {
//
//        @Override
//        public void setup(Context context) throws IOException {
//            if (time == 0) {
//                try {
//                    Path path = new Path(context.getConfiguration().get("name_list", null));
//                    FileReader fr = new FileReader(path.toString());
//                    BufferedReader bf = new BufferedReader(fr);
//                    String str;
//                    int i = 0;
//                    while ((str = bf.readLine()) != null) {
//                        str = str.replace("\n", "");
//                        preLabel.put(str, i);
//                        i++;
//                    }
//                    bf.close();
//                    fr.close();
//                } catch (IOException ex) {
//                    ex.printStackTrace();
//                }
//            }
//            else {
//                preLabel = label;
//                label.clear();
//            }
//        }
//
//        @Override
//        public void map(Object key, Text value, Context context)
//                throws IOException, InterruptedException {
//            String[] itr = value.toString().split("\t");
//            String curr = itr[0];
//            String[] neighbors = itr[1].split("\\|");
//            HashMap<Integer, Double> labelWeights = new HashMap<Integer, Double>();
//            for (int i = 0; i < neighbors.length; i++) {
//                String[] neighbor = neighbors[i].split(",");
//                String name = neighbor[0];
//                double weight = Double.parseDouble(neighbor[1]);
//                if (labelWeights.containsKey(preLabel.get(name))) {
//                    double sum = labelWeights.get(preLabel.get(name));
//                    sum += weight;
//                    labelWeights.put(preLabel.get(name), sum);
//                }
//                else {
//                    labelWeights.put(preLabel.get(name), weight);
//                }
//            }
//
//            double max = -1;
//            Integer l = -1;
//            for (Integer k: labelWeights.keySet()) {
//                if (labelWeights.get(k) > max) {
//                    max = labelWeights.get(k);
//                    l = k;
//                }
//            }
//
//
//            label.put(curr, l);
//            context.write(new Text(curr), new Text(l.toString()));
//        }
//
////        public void cleanup(Context context) throws IOException {
////            FSDataOutputStream out = hdfs.create(new Path("./RawTag" + time +".txt"));
////            PrintWriter pr0 = new PrintWriter(out);
////            Set<Map.Entry<String, Integer>> ms = label.entrySet();
////            for (Map.Entry<String, Integer> entry : ms) {
////                String name = entry.getKey();
////                int label = entry.getValue();
////                pr0.println(new String(name + "\t" + label));
////            }
////
////            pr0.close();
////            out.close();
////        }
//    }
//
//    public static class LPAReducer extends Reducer<Text, Text, Text, Text> {
//        @Override
//        public void reduce(Text key, Iterable<Text> values, Context context)
//            throws IOException, InterruptedException {
//            for (Text v : values)
//                context.write(key, v);
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: LPA of <in> <out>");
//            System.exit(2);
//        }
//        conf.set("name_list","task2/people_name_list.txt");
//        for (; time < iterTime; time++) {
//            Job job = Job.getInstance(conf, "LPA");
//            job.setJarByClass(task5.class);
//            job.setMapperClass(LPAMapper.class);
//            //job.setReducerClass(LPAReducer.class);
//
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
//
//            FileInputFormat.addInputPath(job, new Path(args[0]));
//            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/res" + time));
//
//            job.waitForCompletion(true);
//        }
//
//    }
    private static int times = 10;

    public static void main(String[] args) throws Exception {
        String[] forInit = {"", args[1] + "/Data0"};
        forInit[0] = args[0];
        InitLabel.run(forInit);

        String[] forItr = {"", ""};
        for (int i = 0; i < times; i++) {
            forItr[0] = args[1] + "/Data" + (i);
            forItr[1] = args[1] + "/Data" + (i+1);
            LPAIter.run(forItr);
        }

        String[] forV = {args[1] + "/Data" + times, args[1]+"/FinalTag"};
        LPAViewer.run(forV);
    }
}
