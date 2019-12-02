import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Client {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//1、配置连接hadoop集群的参数
Configuration conf = new Configuration();
conf.set("fs.defaultFS","hdfs://mycluster");
//2、获取job对象实例
Job job = Job.getInstance(conf,"wordcount");
//3、指定本业务job的路径
job.setJarByClass(Client.class);
//4、指定本业务job要使用的Mapper类
job.setMapperClass(MyMapper.class);
//5、指定mapper类的输出数据的kv的类型
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
//6、指定本业务job要使用的Reducer类
job.setReducerClass(MyReduce.class);
//7、设置程序的最终输出结果的kv的类型
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(LongWritable.class);
FileInputFormat.setInputPaths(job,new Path(args[0]));
//9、设置job的输出目录
FileOutputFormat.setOutputPath(job,new Path(args[1]));
//10、提交job
// job.submit();
boolean b = job.waitForCompletion(true);
 System.exit(b?0:1);


    }


}
