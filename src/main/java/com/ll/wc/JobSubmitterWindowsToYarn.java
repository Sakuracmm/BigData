package com.ll.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 用于提交mapReduce job的客户端程序
 * 功能：
 *      1、封装本地job运行时需要的必要参数
 *      2、跟yarn进行交互，将mapReduce程序成功地启动运行
 */
public class JobSubmitterWindowsToYarn {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //在代码中设置JVM系统参数，用于给job对象来获取访问hdfs的用户身份
        System.setProperty("HADOOP_USER_NAME", "root");


        Configuration conf = new Configuration();

        //1、设置job运行时要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://master:9000");
        //2、设置job提交到哪里去运行
        conf.set("mapreduce.framework.name", "yarn");
        //3、设置yarn所在路径 告诉它yarn在哪里
        conf.set("yarn.resourcemanager.hostname","master");
        //平台兼容性解决办法一：4、如果要从Windows系统上运行这个job及提交客户端程序，则需要加上这个跨平台参数
        conf.set("mapreduce.app-submission.cross-platform","true");
        //平台兼容性解决办法二;将项目打成jar包拷贝到linux程序下编译运行

        Job job = Job.getInstance(conf);

        //1、封装参数，jar包所在的位置
        job.setJar("C:\\Users\\Administrator\\Workspaces\\IDEA\\mapreduce-maven\\target\\mapreduce-1.0-SNAPSHOT.jar");
        //动态获取到main方法执行所在目录，并且将jar打在该目录下
        //job.setJarByClass(JobSubmitterWindowsToYarn.class);

        //2、封装参数，本次job需要调用哪个Mapper实现类、Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //3、封装参数，本次job的Mapper实现类、Reducer实现类产生的结果数据的key,value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path output = new Path("/wordcount/output/");
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000/"),conf,"root");
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        //4、封装参数，本次job需要处理的数据集所在目录,以及本次处理结果的输出路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, output);        //注意，输出路径必须不存在，否则会抛异常

        //5、封装参数，想要启动的reduce task的数量
        job.setNumReduceTasks(2);   //默认是1个

        //6、提交job给yarn
//        job.submit();
        boolean success = job.waitForCompletion(true);

        System.exit(success ? 0 : -1);
    }
}
