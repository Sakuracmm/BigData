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
import java.util.ArrayList;
import java.util.Collections;

public class CommonFriendsOne {

    public static class CommonFriendsOneMapper extends Mapper<LongWritable,Text,Text, Text>{
        Text k = new Text();
        Text v = new Text();

        //输入:A:B,C,D,E,F,O
        //输出:B->A,C->A,D->A,E->A,F->A,O->A
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,Text>.Context context)
                throws IOException, InterruptedException {
            String[] userAndFriends = value.toString().split(":");
            String user = userAndFriends[0];
            String[] friends = userAndFriends[1].split(",");
            v.set(user);
            for (String f:
                 friends) {
                k.set(f);
                context.write(k,v);
            }
        }
    }


    public static class CommonFriendsOneReducer extends Reducer<Text,Text,Text,Text>{

        //一组数据： B --> A E F J......
        //一组数据： C --> B F E J......
        @Override
        protected void reduce(Text friend, Iterable<Text> users, Reducer<Text,Text,Text,Text>.Context context)
                throws IOException, InterruptedException {

            ArrayList<String> userList = new ArrayList<String>();

            for(Text user : users){
                userList.add(user.toString());
            }

            Collections.sort(userList);

            for(int i = 0 ; i <= userList.size() - 1;i++){
                for(int j = i + 1; j <= userList.size() - 1; j++ ){
                    context.write(new Text(userList.get(i)+"-"+userList.get(j)),friend);
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("fs.default","hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","192.168.234.11");

         Job job = Job.getInstance(conf);
         job.setJarByClass(CommonFriendsOne.class);

         job.setMapOutputValueClass(Text.class);
         job.setMapOutputKeyClass(Text.class);

         job.setOutputValueClass(Text.class);
         job.setOutputKeyClass(Text.class);

         job.setMapperClass(CommonFriendsOneMapper.class);
         job.setReducerClass(CommonFriendsOneReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/friend/input/"));
        FileOutputFormat.setOutputPath(job,new Path("/friend/output/"));

        job.waitForCompletion(true);
    }

}
