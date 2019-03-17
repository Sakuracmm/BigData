package com.ll.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * 本例使用的是较为low的方式实现，还可以使用优化的解决方案，利用Partitioner+CompareTo+GroupingComparator组合来高效实现
 */
public class ReduceSideJoin {

    public static class ReduceSideJoinMapper extends Mapper<LongWritable,Text, Text,JoinBean> {
        String name = null;
        JoinBean bean = new JoinBean();
        Text k = new Text();
        /**
         * map task在做数据处理时，会先调用一次setup
         * 调用完之后才对每一行反复调用map方法
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Mapper<LongWritable,Text, Text,JoinBean>.Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            name = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable,Text, Text,JoinBean>.Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if(name.startsWith("order")){
                bean.set(fields[0],fields[1],"NULL",-1,"NULL","order");
            }else{
                bean.set("NUll",fields[0],fields[1],Integer.parseInt(fields[2]),fields[3],"user");
            }
            k.set(bean.getUserId());
            context.write(k,bean);
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Reducer<Text,JoinBean,JoinBean,NullWritable>.Context context)
                throws IOException, InterruptedException {
            ArrayList<JoinBean> orderList = new ArrayList<JoinBean>();
            JoinBean userBean = null;
            try {
                //区分两类数据
                for (JoinBean value:
                 values) {
                    if("order".equals(value.getTableName())) {
                        JoinBean newBean = new JoinBean();
                        BeanUtils.copyProperties(newBean, value);
                        orderList.add(newBean);
                    }else{
                        userBean = new JoinBean();
                        BeanUtils.copyProperties(userBean,value);
                    }
                }
                //拼接数据并输出
                for(JoinBean bean : orderList){
                    bean.setUserName(userBean.getUserName());
                    bean.setUserAge(userBean.getUserAge());
                    bean.setUserFriend(userBean.getUserFriend());

                    context.write(bean,NullWritable.get());
                }
            }catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default","hdfs://master:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","master");

        Job job = Job.getInstance(conf);
        job.setJarByClass(ReduceSideJoin.class);

        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/join/input/"));
        FileOutputFormat.setOutputPath(job,new Path("/join/output/"));

        job.waitForCompletion(true);

    }

}
