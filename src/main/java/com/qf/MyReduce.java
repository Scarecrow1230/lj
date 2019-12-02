package com.qf;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MyReduce extends Reducer<Bean, NullWritable,Bean,NullWritable> {
    @Override
    protected void reduce(Bean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
