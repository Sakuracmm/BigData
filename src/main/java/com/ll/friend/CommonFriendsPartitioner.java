package com.ll.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CommonFriendsPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text text, Text text2, int numPartitions) {
        String users = text.toString();
        String[] user = users.split("-");
        return (user[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
