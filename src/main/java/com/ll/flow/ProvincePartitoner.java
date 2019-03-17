package com.ll.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 本类是提供给MapTask使用的
 * MapTask通过这个类的getPartition方法来计算它所产生的每一对kv键值对分发给指定的reduce task
 * 控制数据分发策略
 */
public class ProvincePartitoner extends Partitioner<Text, FlowBean> {
    static HashMap<String, Integer> codemap = new HashMap<String, Integer>();

    static {
        codemap.put("135", 0);
        codemap.put("136", 1);
        codemap.put("137", 2);
        codemap.put("138", 3);
        codemap.put("139", 4);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        Integer code = codemap.get(text.toString().substring(0,3));
        return code == null ?  5 : code;
    }
}
