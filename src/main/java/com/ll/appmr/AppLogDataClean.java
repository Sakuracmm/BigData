package com.ll.appmr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.zookeeper.ZooDefs.OpCode.multi;

//数据清洗
public class AppLogDataClean {

    public static class AppLogDataCleanMapper extends Mapper<LongWritable, Text,Text, NullWritable>{

        Text k = null;
        NullWritable v = null;
        SimpleDateFormat sdf  = null;
        MultipleOutputs<Text,NullWritable> mos = null;  //多路输出器
        @Override
        protected void setup(Mapper<LongWritable,Text,Text,NullWritable>.Context context)
                throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text,NullWritable>(context);
            k = new Text();
            v = NullWritable.get();
            sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,NullWritable>.Context context)
                throws IOException, InterruptedException {

            JSONObject jsonObj = JSON.parseObject(value.toString());
            JSONObject headerObj = jsonObj.getJSONObject(GlobalConstants.HEADER);

            /**
             * 过滤缺失必选字段的信息
             */
            if(StringUtils.isBlank(headerObj.getString("sdk_ver"))){
                return;
            }
            if(null == headerObj.getString("time_zone") || "".equals(headerObj.getString("time_zone").trim())){
                return;
            }
            if(null == headerObj.getString("commit_id") || "".equals(headerObj.getString("commit_id").trim())){
                return;
            }
            if(null == headerObj.getString("commit_time") || "".equals(headerObj.getString("commit_time").trim())){
                return;
            }else{
                //练习中添加的逻辑，替换掉原始数据中的时间戳
                String commit_time = headerObj.getString("commit_time");
                String format = sdf.format(new Date(Long.parseLong(commit_time)));
                headerObj.put("commit_time",format);
            }
            if(null == headerObj.getString("pid") || "".equals(headerObj.getString("pid").trim())){
                return;
            }
            if(null == headerObj.getString("app_token") || "".equals(headerObj.getString("app_token").trim())){
                return;
            }
            if(null == headerObj.getString("app_id") || "".equals(headerObj.getString("app_id").trim())){
                return;
            }
            if(null == headerObj.getString("device_id") || headerObj.getString("device_id").length() < 17){
                return;
            }
            if(null == headerObj.getString("device_id_type") || "".equals(headerObj.getString("device_id_type").trim())){
                return;
            }
            if(null == headerObj.getString("release_channel") || "".equals(headerObj.getString("release_channel").trim())){
                return;
            }
            if(null == headerObj.getString("app_ver_name") || "".equals(headerObj.getString("app_ver_name").trim())){
                return;
            }
            if(null == headerObj.getString("app_ver_code") || "".equals(headerObj.getString("app_ver_code").trim())){
                return;
            }
            if(null == headerObj.getString("os_name") || "".equals(headerObj.getString("os_name").trim())){
                return;
            }
            if(null == headerObj.getString("os_ver") || "".equals(headerObj.getString("os_ver").trim())){
                return;
            }
            if(null == headerObj.getString("language") || "".equals(headerObj.getString("language").trim())){
                return;
            }
            if(null == headerObj.getString("country") || "".equals(headerObj.getString("country").trim())){
                return;
            }
            if(null == headerObj.getString("manufacture") || "".equals(headerObj.getString("manufacture").trim())){
                return;
            }
            if(null == headerObj.getString("device_model") || "".equals(headerObj.getString("device_model").trim())){
                return;
            }
            if(null == headerObj.getString("resolution") || "".equals(headerObj.getString("resolution").trim())){
                return;
            }
            if(null == headerObj.getString("net_type") || "".equals(headerObj.getString("net_type").trim())){
                return;
            }
            /**
             * 生成userId
             */
            String user_id="";
            if("android".equals(headerObj.getString("os_name"))){
                user_id = StringUtils.isNotBlank(headerObj.getString("android_id"))?headerObj.getString("android_id"):headerObj.getString("device_id");
            }else{
                user_id=headerObj.getString("device_id");
            }

            /**
             * 输出结果
             */

            headerObj.put("user_id",user_id);
            k.set(JsonToStringUtil.toString(headerObj));
            if("android".equals(headerObj.getString("os_name"))){
                mos.write(k,v,"android/android");
            }else{
                mos.write(k,v,"ios/ios");
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.framework.hostname","master");

        Job job = Job.getInstance(conf);

        job.setJarByClass(AppLogDataClean.class);

        job.setMapperClass(AppLogDataCleanMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

//        job.setOutputFormatClass(TextOutputFormat.class);  --->默认
        //解决输出目录下的空文件的办法        --->配合上面的MultipleOutputs多路输出器
        //避免生成默认的part-r-000000等文件，因为已经交给MultipleOutputs去输出了
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
