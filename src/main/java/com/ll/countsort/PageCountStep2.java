package com.ll.countsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageCountStep2 {

    public static class PageCountStep2Mapper extends Mapper<LongWritable, Text,PageCount, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            PageCount pageCount = new PageCount();

            System.out.println("长度："+split.length);
            System.out.println("第一个子串："+split[0]);

            pageCount.set(split[0],Integer.parseInt(split[1]));

            context.write(pageCount,NullWritable.get());
        }
    }

    public static class PageCountStep2Reducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable>{
        @Override
        protected void reduce(PageCount key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resource.hostname","master");

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountStep2.class);

        job.setMapperClass(PageCountStep2Mapper.class);
        job.setReducerClass(PageCountStep2Reducer.class);

        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/pagecountsort/output"));
        FileOutputFormat.setOutputPath(job, new Path("/pagecountsort/sort"));

        job.setNumReduceTasks(1);

        boolean wait = job.waitForCompletion(true);
        System.exit(wait ? 0 : -1);


    }

}
