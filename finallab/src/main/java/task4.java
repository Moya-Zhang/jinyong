import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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


public class task4 {
    //GraphBuilder
    public static class GraphBuilder{
        public static void run(String arg[])
                throws IOException,InterruptedException,ClassNotFoundException
        {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, arg).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: InvertedIndex <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "graphbuilder");
            job.setJarByClass(GraphBuilder.class);
            job.setMapperClass(GraphBuilderMapper.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(arg[0]));
            FileOutputFormat.setOutputPath(job, new Path(arg[1]));
            
            job.waitForCompletion(true);
        }
        public static class GraphBuilderMapper 
                extends Mapper<Object,Text,Text,Text>
        {
            public void map(Object key,Text value,Context context)
                    throws IOException,InterruptedException
            {
                //name+\t+1.0+\t+name1,pr1|name2,pr2......
                String pagerank="1.0\t";
                String s=value.toString();
                Text page=new Text(s.split("\t")[0]);
                pagerank+=s.split("\t")[1];
                context.write(page,new Text(pagerank));
            }
        }
    }
    //PagerankIter
    public static class PageRankIter{
        public static void run(String arg[])
                throws IOException,InterruptedException,ClassNotFoundException
        {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, arg).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: InvertedIndex <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "pagerankiter");
            job.setJarByClass(PageRankIter.class);
            job.setMapperClass(PageRankIterMapper.class);
            job.setReducerClass(PageRankIterReducer.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(arg[0]));
            FileOutputFormat.setOutputPath(job, new Path(arg[1]));

            job.waitForCompletion(true);
        }
        public static class PageRankIterMapper
            extends Mapper<LongWritable,Text,Text,Text>
        {
            public void map(LongWritable key,Text value,Context context)
                    throws IOException,InterruptedException
            {
                String line=value.toString();
                String[] tuple=line.split("\t");
                String pageKey=tuple[0];//name
                double pr=Double.parseDouble(tuple[1]);
                if(tuple.length>2){
                    String[] linkpages=tuple[2].split("\\|");
                    for(String linkpage:linkpages){
                        String[] tmp=linkpage.split(",");
                        double ratio=Double.parseDouble(tmp[1]);
                        String prValue=pageKey+"\t"+String.valueOf(ratio*pr);
                        //将传递给该人物的pr值进行输出
                        context.write(new Text(tmp[0]),new Text(prValue));
                    }
                    //传递原始链接信息
                    context.write(new Text(pageKey),new Text("|"+tuple[2]));
                }
            }
        }
        public static class PageRankIterReducer
                extends Reducer<Text,Text,Text,Text>
        {
            public void reduce(Text key,Iterable<Text> values,Context context)
                    throws IOException,InterruptedException
            {
                String links="";
                double pagerank=0;
                for(Text value:values){
                    String tmp=value.toString();
                    if(tmp.startsWith("|")){
                        links="\t"+tmp.substring(tmp.indexOf("|")+1);
                        continue;
                    }
                    String[] tuple=tmp.split("\t");
                    if(tuple.length>1)
                        pagerank+=Double.parseDouble(tuple[1]);
                }
                //double damping=0.85;
                //pagerank=(double)(1-damping)+damping*pagerank;
                context.write(new Text(key),new Text(String.valueOf(pagerank)+links));
            }
        }
    }
    
    //PagerankViewer
    public static class PageRankViewer{
        public static class myComparator extends DoubleWritable.Comparator {
            @SuppressWarnings("rawtypes")
            public int compare( WritableComparable a,WritableComparable b){
                return -super.compare(a, b);
            }
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
        
        public static void run(String arg[])
                throws IOException,InterruptedException,ClassNotFoundException
        {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, arg).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: InvertedIndex <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "pagerankviewer");
            job.setJarByClass(PageRankViewer.class);
            job.setMapperClass(PageRankViewerMapper.class);

            job.setSortComparatorClass(myComparator.class);           //自定义排序
            
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(arg[0]));
            FileOutputFormat.setOutputPath(job, new Path(arg[1]));

            job.waitForCompletion(true);
        }
        public static class PageRankViewerMapper
            extends Mapper<LongWritable,Text, DoubleWritable,Text>
        {

            public void map(LongWritable key,Text value,Context context)
                    throws IOException,InterruptedException
            {
                String[] line=value.toString().split("\t");
                String page=line[0];
                double pr=Double.parseDouble(line[1]);
                context.write(new DoubleWritable(pr),new Text(page));
            }
        }
    }
    
    //driver
    private static int times=20;
    public static void main(String args[]) throws Exception
    {
        String[] forGB = {"", args[1]+"/Data0"};
        forGB[0] = args[0];
        GraphBuilder.run(forGB);

        String[] forItr = {"",""};
        for (int i=0; i<times; i++) {
            forItr[0] = args[1]+"/Data"+(i);
            forItr[1] = args[1]+"/Data"+(i+1);
            PageRankIter.run(forItr);
        }

        String[] forRV = {args[1]+"/Data"+times, args[1]+"/FinalRank"};
        PageRankViewer.run(forRV);
    }
}
