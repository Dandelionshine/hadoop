
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class CreatLink
{
    public static class CreatLinkMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        Text outkey=new Text();
        Text outvalue=new Text();

        protected void map(LongWritable key,Text value,Context context)
                throws IOException, InterruptedException
        {
            String line=value.toString();
            String split[]=line.split("\t");
            outkey.set(split[0]);

            String value1="a"+split[1];
            outvalue.set(value1);

            context.write(outkey,outvalue);

            if (split[1].contains(","))
            {
                String sp2[]=split[1].split(",");
                for(int i=0;i<sp2.length-1;i++)
                {
                    for (int j=i+1;j<sp2.length;j++)
                    {
                        outkey.set(sp2[i]);
                        outvalue.set(sp2[j]);
                        context.write(outkey,outvalue);
                    }
                }
            }
        }
    }

    public static class CreatLinkReducer extends Reducer<Text,Text,IntWritable,Text>
    {
        static IntWritable outkey=new IntWritable();
        static Text outvalue=new Text("");
        static int l=0;
        protected void reduce(Text key,Iterable<Text>values,Context context)
                throws IOException,InterruptedException
        {
            ArrayList<String> list=new ArrayList<String>();
            HashMap<String,Boolean> map=new HashMap<String, Boolean>();
            for (Text val:values)
            {
                String v=val.toString();
                if(v.startsWith("a"))
                {
                    String v2=v.substring(1);
                    String sp[]=v2.split(",");
                    for (int i=0;i<sp.length;i++)
                    {
                        map.put(sp[i],true);
                    }
                }
                else
                {
                    list.add(v);
                }
            }
            for(int i=0;i<list.size();i++)
            {
                if(map.containsKey(list.get(i)))
                {
                    l++;
                }
            }
            outkey.set(l);
        }

        protected void cleanup(Reducer<Text,Text,IntWritable,Text>.Context context)
                throws IOException,InterruptedException
        {
            context.write(outkey,outvalue);
        }
    }

    public static void main(String[]args)throws Exception
    {
        Configuration conf=new Configuration();
        Job job2=new Job(conf,"CreatLink");
        job2.setNumReduceTasks(1);
        job2.setJarByClass(CreatLink.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(CreatLinkMapper.class);
        job2.setReducerClass(CreatLinkReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
    }
}
