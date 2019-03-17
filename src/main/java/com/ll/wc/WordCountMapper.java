package com.ll.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: 是map task读取到的数据key的类型,是一行的起始偏移量，是一个long
 * VALUEIN: 是map task读取到的数据的value的类型，是一行的内容，String
 *
 * KETOUT: 是用户自定义的map方法哟啊返回的kv键值对中的key的类型，在wordcount中，我们需要返回的是单词String
 * VALUEOUT: 是用户自定义的map方法哟啊返回的kv键值对中的value的类型，在wordcount中，我们需要返回的是整数Integer
 *
 * 但是，在MapReduce中，map产生的数据需要传输给reduce,需要进行序列化，而jdk中的原生序列化机制产生的数据量比较
 * 冗余，就会导致MapReduce运行效率低下，所以Hadoop专门设计了自己的序列化机制，那么，MapReduce中传输的数据类型
 * 就必须实现Hadoop自己的序列化接口
 *
 * 所以Hadoop为jdk中的常用基本类型Long，String, Integer, Float..等数据类型封装了自己实现了的Hadoop序列化的接口类
 * 型：LongWriteable、Text、IntegerWriteable、FloatWriteable...(除了String“与众不同”)
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //切单词
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word: words) {
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
