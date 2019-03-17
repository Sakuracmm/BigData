package com.ll.skew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class SkewWordcount2 {

    public static class SkewWordcountMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] wordAndCount = value.toString().split("\t");
            v.set(Integer.parseInt(wordAndCount[1]));
            k.set((wordAndCount[0].split("\001"))[0]);
            context.write(k,v);
        }
    }

    public static class SkewWordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        IntWritable v = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value:
                 values) {
                count += value.get();
            }
            v.set(count);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default","hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","yarn");

        Job job = Job.getInstance(conf);
        job.setJarByClass(SkewWordcount2.class);

        //设置map task的局部聚合逻辑类
        job.setCombinerClass(SkewWordcountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SkewWordcountMapper.class);
        job.setReducerClass(SkewWordcountReducer.class);

        FileInputFormat.setInputPaths(job,new Path("/wordcount/output4"));
        FileOutputFormat.setOutputPath(job,new Path("/wordcount/output5"));

        job.setNumReduceTasks(3);
        job.waitForCompletion(true);
    }

}
