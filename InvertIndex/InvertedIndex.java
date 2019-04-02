package com.company;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex{
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private Text word = new Text();
        // private String pattern = "[^\\w]";
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String temp=new String();
            String line=value.toString().toLowerCase();
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String filename=fileSplit.getPath().getName();
            int dot=filename.indexOf(".");
            filename=filename.substring(0,dot);
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                temp=itr.nextToken();
                word.set(temp+"#"+filename);
                context.write(word,new IntWritable(1));
            }
        }
    }


    public static class SumCombineer
            extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private Text word=new Text();
        private IntWritable result=new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class NewPartitioner extends HashPartitioner<Text,IntWritable>{
        public int getPartitioner(Text key,IntWritable value,int numReduceTasks){
            String term=new String();
            term=key.toString().split(",")[0];// <term#docid>=>term
            return super.getPartition(new Text(term),value,numReduceTasks);
        }
    }
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,Text> {
        private IntWritable result = new IntWritable();
        private Text word1=new Text();
        private Text word2=new Text();
        String temp=new String();
        static Text CurrentItem=new Text(" ");
        static List<String> postingList=new ArrayList<String>();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split("#")[0]);
            temp =key.toString().split("#")[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            word2.set("<"+temp+":"+sum+">");
            if(!CurrentItem.equals(word1)&&!CurrentItem.equals(" ")){
                StringBuilder out=new StringBuilder();
                StringBuilder out1=new StringBuilder();
                long count=0;
                int freq=0;
                for (String p :
                        postingList) {
                    freq++;
                    String p1=new String();
                    p1=p.substring(p.indexOf("<")+1,p.indexOf(">"));
                    out1.append(p1);
                    out1.append(";");
                    count+=
                            Long.parseLong(p.substring(p.indexOf(":")+1,p.indexOf(">")));
                }
                if(freq!=0){
                    double ave=count/freq;
                    out.append(" "+ave+",");
                }


                out.append(out1);
                out1.delete(0,out1.length());

                if(count>0){
                    context.write(CurrentItem,new Text(out.toString()));
                }
                postingList=new ArrayList<String>();

            }
            CurrentItem=new Text(word1);
            postingList.add(word2.toString());
        }
        public void cleanup(Context context)
                throws IOException,InterruptedException{
            StringBuilder out1=new StringBuilder();
            StringBuilder out=new StringBuilder();
            long count=0;
            int filecount=0;
            for (String p :
                    postingList) {

                String p1=new String();
                p1=p.substring(p.indexOf("<")+1,p.indexOf(">"));
                out1.append(p1);
                out1.append(";");
                count+=
                        Long.parseLong(p.substring(p.indexOf(":")+1,p.indexOf(">")));
                filecount++;
            }
            if(filecount!=0){
                double ave=count/filecount;
                out.append(" "+ave+",");
            }
            out.append(out1);
            out1.delete(0,out1.length());
            if(count>0)
                context.write(CurrentItem,new Text(out.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        // job.setInputFormatClass(FileNameInputFormat.class);


        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumCombineer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setPartitionerClass(NewPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
