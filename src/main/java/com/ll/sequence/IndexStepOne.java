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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

//import org.apache.hadoop.mapred.FileSplit;

public class IndexStepOne {

    public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        /**
         * 格式<单词-文件名， 1>
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //获得输入切片
            //用于描述maptask所处理的数据的任务范围，
            //如果maptask处理的是文件，那么应该包括一下信息描述：
            //文件路径、偏移量范围
            //如果maptask读的数据库里面的数据
            //划分任务范围应该用如下的信息描述：
            //表名、库名，行范围
            //所以给出的返回对象是一个抽象对象
            //InputSplit inputSplit = context.getInputSplit();
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String filename = inputSplit.getPath().getName();
            //从输入切面信息中读取当前正在进行处理的一些数据所需文件
            String[] words = value.toString().split(" ");
            for (String w: words) {
                //将“单词-文件名”作为key，1作为value输出
                context.write(new Text(w + "-" + filename), new IntWritable(1));
            }
        }
    }

    public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value : values){
                count += value.get();
            }
            context.write(new Text(key), new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        //默认只加载core-default.xml、core-site.xml
        conf.set("fs.defaultFS","hdfs://master:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","master");

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepOne.class);

        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //这是默认的输出组件
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("/index/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/index/output-seq-1/"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);


    }


}
