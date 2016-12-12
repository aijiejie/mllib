package com.li.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TestNewCombinerGrouping;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by shaohui on 2016/12/9 0009.
 */
public class Runjob {
    static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static class hotMapper extends Mapper<LongWritable,Text,keypair,Text>{

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] ss = line.split("\t");
            if (ss.length == 2){
                try {
                    Date date = SDF.parse(ss[0]);
                    Calendar c = Calendar.getInstance();
                    c.setTime(date);
                    int year = c.get(1);
                    String hot = ss[1].substring(0,ss[1].indexOf("℃"));
                    keypair kp = new keypair();
                    kp.setYear(year);
                    kp.setHot(Integer.parseInt(hot));
                    context.write(kp,value);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    static class hotreduce extends Reducer<keypair,Text,keypair,Text>{
        @Override
        protected void reduce(keypair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v :values) {
                context.write(key,v);
            }
        }
    }
    public static void main(String[] args){
        Configuration conf = new Configuration();
        try {
            Job job = new Job(conf);
            job.setJobName("hot");
            job.setJarByClass(Runjob.class);
            job.setMapperClass(hotMapper.class);
            job.setReducerClass(hotreduce.class);
            job.setMapOutputKeyClass(keypair.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(3);
            job.setPartitionerClass(FirstPartition.class);//设置分区类
            job.setSortComparatorClass(Sortkey.class);//设置排序类
            job.setGroupingComparatorClass(group.class);//设置分组类
            FileInputFormat.addInputPath(job,new Path("hdfs:///ceshi/hotdata"));
            FileOutputFormat.setOutputPath(job,new Path("hdfs:///usr/output/hotdata"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
