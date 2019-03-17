package com.ll.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        //设置job运行时要访问的默认文件系统
        conf.set("fs.defaultFS","hdfs://master:9000");
        //设置job要提交到哪里去运行
        conf.set("mapreduce.framework.name","yarn");
        //设置yarn所在的路径，告诉它yarn在哪里
        conf.set("mapreduce.resourcemanager.hostname","master");

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        //设置参数： maptask在做数据分区时，用哪个分区de的逻辑类
        //如果不指定，则会使用默认的HashPartitioner
      job.setPartitionerClass(ProvincePartitoner.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //由于我们的ProvincePartitioner可能会产生6种分区号，所以，需要有6个reduce task来处理
        job.setNumReduceTasks(6);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        FileInputFormat.setInputPaths(job, new Path("/flowcount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/flowcount/ProvinceOutput/"));

        job.waitForCompletion(true);
    }
}
