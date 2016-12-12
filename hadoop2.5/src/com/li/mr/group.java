package com.li.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by shaohui on 2016/12/9 0009.
 */
public class group extends WritableComparator {
    public group(){
        super(keypair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        keypair o1=(keypair) a;
        keypair o2=(keypair) b;
        return Integer.compare(o1.getYear(),o2.getYear());
    }
}
