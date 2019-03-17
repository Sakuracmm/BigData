package com.ll.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 如果要在hadoop集群的某台机器上启动这个job提交客户端的话
 * conf里面不需要指定fs.defaultFS  mapreduce.framework.name
 *
 * 因为在集群机器上用hadoop jar xx.jar com.ll.wc.JobSubmitterLinuxToYarn 命令来启动客户端的方法时，
 * Hadoop jar 这个明亮会将所在机器上的hadoop安装目录中的jar包中所有的配置文件加入运行时的
 * claspath中
 *
 * name，我们在客户端main方法中的new Configuration()语句就会自动加载classpath中的配置文件，
 * 自然就会有fs.defaultFs和mapreduce.framework.name 和yarn.resourcemanager.hostname这些配置参数
 */
public class JobSubmitterWindowsLocal {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        //设定默认文件系统为本地windows文件系统
        //conf.set("fs.defaultFS","file:///");
        //设置指定MapReduce-job提交到哪去运行----这里提交到本地运行
        //conf.set("mapreduce.framework.name","local");

        //conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance();

        job.setJarByClass(JobSubmitterLinuxToYarn.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("f:/mrdata/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("f:/mrdata/wordcount/output/"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);


    }

}
