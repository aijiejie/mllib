package com.li.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by shaohui on 2016/12/9 0009.
 */
public class Sortkey extends WritableComparator {
    public Sortkey(){
        super(keypair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        keypair o1=(keypair) a;
        keypair o2=(keypair) b;
        int res = Integer.compare(o1.getYear(),o2.getYear());
        if (res!=0) return res;
        return Integer.compare(o1.getHot(),o2.getHot());
    }
}
