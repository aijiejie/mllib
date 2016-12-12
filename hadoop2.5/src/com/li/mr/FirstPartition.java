package com.li.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by shaohui on 2016/12/9 0009.
 */
public class FirstPartition extends Partitioner<keypair,Text> {


    @Override
    public int getPartition(keypair kp, Text text, int i) {
        return (kp.getYear()*127) % i;
    }
}
