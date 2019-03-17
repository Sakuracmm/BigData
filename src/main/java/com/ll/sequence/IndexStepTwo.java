package com.ll.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//import org.apache.hadoop.mapred.FileSplit;

public class IndexStepTwo {

    public static class IndexStepTwoMapper extends Mapper<Text, IntWritable, Text, Text>{
        /**
         * hello-c.txt	4      ===>>       (key,value) => (hello,c.txt-->4)
         * key值相同，value可以不同的聚合在一起处理
         */
        @Override
        protected void map(Text key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            String[] split = key.toString().split("-");
            context.write(new Text(split[0]),new Text(split[1] + "--->" + value));
        }
    }

    public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
        // 一组数据 <hello, a.txt-->4>
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //StringBuffer是线程安全的，StringBuilder是非线程安全的，在不涉及
            //线程安全的场景下使用StringBuilder更快
            StringBuilder sb = new StringBuilder();
            for(Text value : values ){
                sb.append(value.toString()).append(" \t");
            }

            context.write(key , new Text(sb.toString()));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        //默认只加载core-default.xml、core-site.xml
        conf.set("fs.defaultFS","hdfs://master:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","master");

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepTwo.class);

        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setInputFormatClass(TextInputFormat.class);//默认的输入组件

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("/index/output/"));
        FileOutputFormat.setOutputPath(job, new Path("/index/output2/"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);


    }


}
