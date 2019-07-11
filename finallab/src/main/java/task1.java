import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.*;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.io.IOException;
import java.util.*;

public class task1 {
    private static List<String> nameDic;

    public static class participleMapper
            extends Mapper<Object, Text, Text, Text> {
        private static Path nameListPath;
        @Override
        public void setup(Context context) throws IOException {
            nameDic = new ArrayList<>();

            try{
                //Path path = new Path(context.getConfiguration().get("name_list", null));
                //FileReader fr = new FileReader(path.toString());
                //FileReader fr = new FileReader(nameListPath.toString());
//                System.out.println(path.toString());
                FileReader fr = new FileReader(DistributedCache.getLocalCacheFiles(context.getConfiguration())[0].toString());
                BufferedReader bf = new BufferedReader(fr);
                //BufferedReader bf = new BufferedReader(fr);
                String str;


                while((str=bf.readLine())!=null){
                    str = str.replace("\n","");
                    nameDic.add(str);
                    DicLibrary.insert(DicLibrary.DEFAULT, str,"nr",1000);//设置自定义分词
                }
                bf.close();
                fr.close();
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //System.out.println("dasdasd\n");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName().split("(.txt)|(.TXT)")[0];
            String temp = value.toString();
            Result result = ToAnalysis.parse(temp);
            List<Term>terms = result.getTerms();
            String res = "";
            for (int i = 0; i < terms.size(); i++) {
                String word = terms.get(i).getName();
                if(nameDic.contains(word)) {
                    if (res.length() == 0) {
                        res = res + word;
                    } else
                        res = res + " " + word;
                }
            }
            if(res.length()==0)
                res = null;

            if(res!=null) {
                Text keyFilename = new Text();
                keyFilename.set(fileName);
                Text valueWords = new Text();
                valueWords.set(res);
                context.write(keyFilename,valueWords);
            }
            //context.write(new Text(fileName),new Text(nameListPath.toString()));
        }
    }

//    public static class participleReducer extends Reducer<Text, Text, Text, Text> {
//        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
//            for(Text t : values)
//                context.write(t,new Text(""));
//        }
//    }

    public static class participleReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
            for(Text value:values){
                Text nullText = new Text();
                nullText.set("");
                multipleOutputs.write(value,nullText,key.toString());
            }
        }
        private MultipleOutputs<Text,Text> multipleOutputs;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
        }
        @Override
        protected void cleanup(Context context)throws IOException,InterruptedException{
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new Path("/data/task2/people_name_list.txt").toUri(), conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: participle of <in> <out>");
            System.exit(2);
        }
        String filename = args[0].substring(args[0].lastIndexOf("/") + 1, args[0].length());
        conf.set("name_list",filename);
        Job job = Job.getInstance(conf, "Participle");
        job.addCacheFile(new Path(args[0]).toUri());
        job.setJarByClass(task1.class);
        job.setMapperClass(participleMapper.class);
        job.setReducerClass(participleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]+"novels"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}

//public class task1{
//    List<String> nameDic;
//    public task1(){
//        nameDic = new ArrayList<String>();
//        try{
//            FileReader fr = new FileReader("Data/people_name_list.txt");
//            BufferedReader bf = new BufferedReader(fr);
//            String str;
//            while((str=bf.readLine())!=null){
//                str = str.replace("\n","");
//                nameDic.add(str);
//                DicLibrary.insert(DicLibrary.DEFAULT, str,"nr",1000);//设置自定义分词
//            }
//            bf.close();
//            fr.close();
//        }catch (IOException ex){
//            ex.printStackTrace();
//        }
//    }
//    public String participle(String str){
//
//
//        Result result = ToAnalysis.parse(str);
//        List<Term>terms = result.getTerms();
//        String res = "";
//        for (int i = 0; i < terms.size(); i++) {
//            String word = terms.get(i).getName();
//
//            if(nameDic.contains(word)){
//                if(res.length()==0) {
//                    res = res + word;
//                }
//                else
//                    res = res+" "+word;
//            }
//
//        }
//        if(res.length()!=0)
//            res+="\n";
//        else
//            res = null;
//        return res;
//    }
//
//    public static void main(String[] args) throws IOException {
//        task1 task = new task1();
//        File folder = new File("novels");
//
//        File[] files = folder.listFiles();
//        for (File file:files) {
//            String outFilePath = "Task1Output/";
//            outFilePath += file.getName();
//            File outFile = new File(outFilePath);
//            if (!outFile.exists()) {
//                outFile.createNewFile();
//            }
//
//            FileWriter fileWriter = new FileWriter(outFilePath,true);
//            FileReader fr = new FileReader(file);
//            BufferedReader reader = new BufferedReader(fr);
//            String str;
//            while((str=reader.readLine())!=null) {
//                String res = null;
//                if((res = task.participle(str))!=null)
//                    fileWriter.write(res);
//            }
//            fileWriter.close();
//            System.out.println(file.getName()+" has been participled.");
//        }
//    }
//}