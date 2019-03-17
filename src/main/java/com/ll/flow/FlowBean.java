package com.ll.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 本案例的功能，演示自定义数据类型如何实现hadoop的序列化接口
 * 1、该类一定要保留空参构造函数
 * 2、Write方法中输出字段二进制数据的顺序，要与readFiles方法读取的顺序一致
 */
public class FlowBean implements Writable {

    private int upFlow;
    private int downFlow;
    private int amountFlow;
    private String phone;

    public FlowBean(){

    }
    public FlowBean(int upFlow, int downFlow, String phone) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.phone = phone;
        this.amountFlow = upFlow + downFlow;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }
    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    /**
     * hadoop系统在序列化该类对象的时候调用的方法
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(amountFlow);
        //dataOutput.write(phone.getBytes());
        dataOutput.writeUTF(phone);

    }

    /**
     * Hadoop在反序列化该类的对象时调用的方法
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.amountFlow = dataInput.readInt();
        this.phone = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.upFlow +"," + this.downFlow + "," + this.amountFlow;
    }
}
