package com.qf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoveDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration configuration = new Configuration(); //系统配置对象
        //        configuration.set("fs.defaultFS","hadoop1:9000");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MoveDriver.class);

        job.setMapperClass(MyMapper2.class);

        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Bean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("f:/input2"));
        FileOutputFormat.setOutputPath(job, new Path("e:/output2"));
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);


    }
}
