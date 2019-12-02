import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class FlowDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, IOException {

        Configuration configuration=new Configuration(); //系统配置对象
//        configuration.set("fs.defaultFS","hadoop1:9000");
        Job job=Job.getInstance(configuration);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlow.class);

        job.setReducerClass(FlowReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneFlow.class);

        FileInputFormat.setInputPaths(job,new Path("f:/input/input.txt"));
        FileOutputFormat.setOutputPath( job,new Path("e:/output") );
        boolean b=job.waitForCompletion(true);

        System.exit(b?0:1);



    }
}
