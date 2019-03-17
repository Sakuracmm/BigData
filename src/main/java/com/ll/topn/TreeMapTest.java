package com.ll.topn;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TreeMapTest {

    public static void main(String[] args) {
        TreeMap<String, Integer> treeMap = new TreeMap<String ,Integer>();
        treeMap.put("a" , 2);
        treeMap.put("b" , 4);
        treeMap.put("c" , 3);
        treeMap.put("d" , 1);
        treeMap.put("ab" , 11);
        Set<Map.Entry<String, Integer>> entries = treeMap.entrySet();
        for (Map.Entry<String , Integer> entry: entries) {
            System.out.println(entry.getKey() + ", " + entry.getValue());
        }
    }




}
