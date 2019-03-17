package com.ll.order;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class kengTest {
    public static void main(String[] args) throws IOException {

        //ArrayList<OrderBean> beans = new ArrayList<OrderBean>();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("d:/keng.dat",true));

        OrderBean bean = new OrderBean();
        bean.set("1", "u","a", 1.0f, 2);
        //beans.add(bean);
        oos.writeObject(bean);

        bean.set("2", "t", " v", 2.0f, 3);
        //beans.add(bean);
        oos.writeObject(bean);

        oos.close();
        //System.out.println(beans);
    }

}
