package com.ll.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * KEYIN: 传入k-v键值对中的key的数据类型
 * VALUEIN: 传入k-v键值对中的value的数据类型
 * KEYOUT 输出k-v键值对中的key的数据类型
 * VALUEOUT: 输出k-v键值对中的value的数据类型
 *
 */

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(new Text(key), new IntWritable(count));
    }
}
