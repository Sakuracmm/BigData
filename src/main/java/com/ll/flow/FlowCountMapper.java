package com.ll.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper <LongWritable, Text, Text, FlowBean>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        String phone = fileds[1];
        int upFlow = Integer.parseInt(fileds[fileds.length - 3]);
        int downFlow  = Integer.parseInt(fileds[fileds.length - 2]);

        context.write(new Text(phone), new FlowBean(upFlow, downFlow, phone));

    }
}
