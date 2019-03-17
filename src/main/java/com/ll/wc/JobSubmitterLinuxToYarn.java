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
public class JobSubmitterLinuxToYarn {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //没有指定默认文件系统
        //没有指定mapreduce.job提交到哪运行，如果不加下面配置，则表示在本地启动，不用提交给yarn
        conf.set("fs.default","hdfs://master:9000/");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.resourcename.hostname","master");

        Job job = Job.getInstance();

        job.setJarByClass(JobSubmitterLinuxToYarn.class);

        //设置maptask的局部聚合看逻辑类
        job.setCombinerClass(WordcountCombiner.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output/"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);


    }

}
