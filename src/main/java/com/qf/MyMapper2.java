package com.qf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
;import java.io.IOException;

public class MyMapper2 extends Mapper<LongWritable, Text,Bean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] s = string.split(" ");
        Bean bean = new Bean(s[0], s[1], s[2], s[3]);
        context.write(bean,NullWritable.get());
    }
}

