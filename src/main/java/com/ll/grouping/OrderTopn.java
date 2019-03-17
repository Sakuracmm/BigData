package com.ll.grouping;

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

public class OrderTopn  {

    public static class OrderTopnMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

        OrderBean orderBean = new OrderBean();
        NullWritable nullWritable = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value,Mapper<LongWritable,Text,OrderBean,NullWritable>.Context context)
                throws IOException, InterruptedException {
            String[] files = value.toString().split(",");
            orderBean.set(files[0],files[1],files[2],Float.parseFloat(files[3]),Integer.parseInt(files[4]));
            context.write(orderBean,nullWritable);
        }

    }

    public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable,OrderBean, NullWritable>{
        /**
         * 虽然reduce方法中的参数key只有一个，但是只要迭代器迭代依次，key中的值就会变
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Reducer<OrderBean, NullWritable,OrderBean,NullWritable>.Context context)
                throws IOException, InterruptedException {
            int i = 0;
            for (NullWritable v:
                 values) {
                context.write(key,v);
                if(++i==3){
                    return;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.resourcename.hostname","master");
        conf.setInt("order.top.n",2);

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopn.class);

        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        job.setNumReduceTasks(2);

        job.setPartitionerClass(OrderIdPartitioner.class);
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/order/input/"));
        FileOutputFormat.setOutputPath(job,new Path("/order/output-03"));

        job.waitForCompletion(true);

    }

}
