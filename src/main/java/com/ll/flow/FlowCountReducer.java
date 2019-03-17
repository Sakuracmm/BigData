package com.ll.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    /**
     * @param key      某个手机号
     * @param values    是这个手机号所产生的访问记录中的流量数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        int upSum = 0;
        int downSum = 0;
        for(FlowBean value: values){
            upSum += value.getUpFlow();
            downSum += value.getDownFlow();
        }

        context.write(key, new FlowBean(upSum, downSum, key.toString()));
    }
}
