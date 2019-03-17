package com.ll.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Properties;

public class JobSubmitter {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 1、通过加载classpath下的*.site.xml文件解析参数，一定要是core-site.xml/core-default.xml/hdfs-core.xml/hdfs-default.xml才回去自动加载
         */
        Configuration conf = new Configuration();
        /**
         * 2、自定xml配置文件，通过conf.addResource("xx.xml")加入其中的配置参数
         */
        conf.addResource("xx-oo.xml");

        //设置默认的文件系统以及访问的地址
        conf.set("fs.defaultFS", "hdfs://master:9000");
         //设置job要提交到哪里去运行
        conf.set("mapreduce.framework.name","yarn");
        //设置yarn所在的路径并告诉它yarn在哪里
        conf.set("mapreduce.resourcemanager.hostname","master");
        /**
         * 通过代码设置参数
         */
//      conf.setInt("top.n",3);
//      conf.setInt("top.n",Integer.parseInt(args[0]));
        /**
         * 通过属性配置文件获取参数
         */
//      Properties props = new Properties();
//      props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
//      conf.setInt("ton.n",Integer.parseInt(props.getProperty("top.n")));

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(PageTopnMapper.class);
        job.setReducerClass(PageTopnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/pagetop/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/pagetop/output/"));

        boolean wait = job.waitForCompletion(true);
        System.exit(wait ? 0 : -1);

    }

}
