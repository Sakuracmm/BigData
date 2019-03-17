package com.ll.flow;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class DataOutputStreamTest {

    public static void main(String[] args) throws IOException {

        DataOutputStream dos = new DataOutputStream(new FileOutputStream(("d:/a.dat")));
        dos.write("我爱你".getBytes("UTF-8"));
        dos.close();

        DataOutputStream dos2 = new DataOutputStream(new FileOutputStream(("d:/b.dat")));
        dos2.writeUTF("我爱你");
        dos2.close();



    }
}
