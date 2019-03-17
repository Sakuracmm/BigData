package com.ll.order;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class OrderBean implements WritableComparable<OrderBean>, Serializable {

    private String orderId;
    private String userId;
    private String pdtName;
    private float price;
    private int number;
    private float amountFee;

    public void set(String orderId, String userId, String pdtName, float price, int number) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
        this.amountFee = price * number;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPdtName() {
        return pdtName;
    }

    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public float getAmountFee() {
        return amountFee;
    }

    public void setAmountFee(float amountFee) {
        this.amountFee = amountFee;
    }

    @Override
    public String toString() {
        return this.orderId + "," + this.userId + "," + this.pdtName + "," + this.price + "," + this.number + "," + this.amountFee;
    }

    //比较规则：先比总金额；如果总金额相等，则比较商品名称
    @Override
    public int compareTo(OrderBean o) {
        return Float.compare(o.getAmountFee(), this.getAmountFee()) == 0 ? this.pdtName.compareTo(o.getPdtName()) : Float.compare(o.getAmountFee(), this.amountFee);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.pdtName);
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.number);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.pdtName = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.number = dataInput.readInt();
        this.amountFee = price * number;
    }
}
