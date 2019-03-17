package com.ll.topn;

import org.apache.hadoop.conf.Configuration;

public class ConfigText {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.addResource("xx-oo.xml");
        System.out.println(conf.get("top.n"));
        System.out.println(conf.get("mygirlfriend"));
        System.out.println(conf.size());

    }

}
