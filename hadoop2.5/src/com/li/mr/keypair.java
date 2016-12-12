package com.li.mr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shaohui on 2016/12/9 0009.
 */
public class keypair implements WritableComparable<keypair> {

    private int year;
    private int hot;

    public keypair(){

    }
    public keypair(int year,int hot){
        setHot(hot);
        setYear(year);
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }



    @Override
    public int compareTo(keypair o) {
        int res=Integer.compare(year,o.year);
        if (res!=0){
            return res;
        }
        return Integer.compare(hot,o.hot);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(hot);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year=dataInput.readInt();
        this.hot=dataInput.readInt();

    }

    @Override
    public String toString() {
        return year+"\t"+hot;
    }

    @Override
    public int hashCode() {
        return new Integer(year+hot).hashCode();
    }
}
