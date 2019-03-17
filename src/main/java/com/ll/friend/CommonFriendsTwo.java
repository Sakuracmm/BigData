package com.ll.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommonFriendsTwo {
    public static class CommonFriendsTwoMapper extends Mapper<LongWritable,Text, Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,Text>.Context context)
                throws IOException, InterruptedException {
            String[] commonFriends = value.toString().split("\t");
            context.write(new Text(commonFriends[0]),new Text(commonFriends[1]));
        }
    }

    public static class CommonFriendsTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder("");
            for (Text value:
                 values) {
                sb.append(" "+value);
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default","hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","master");

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendsTwo.class);

        job.setPartitionerClass(CommonFriendsPartitioner.class);

        job.setMapperClass(CommonFriendsTwoMapper.class);
        job.setReducerClass(CommonFriendsTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("/friend/output/"));
        FileOutputFormat.setOutputPath(job,new Path("/friend/output-3/"));

        job.setNumReduceTasks(2);
        job.waitForCompletion(true);
    }

}
