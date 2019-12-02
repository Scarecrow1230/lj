import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,PhoneFlow> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] s = string.split("  ");
        int upFlow=Integer.parseInt(s[2]);
        int downFlow=Integer.parseInt(s[3]);
        int sumFlow=upFlow+downFlow;
        PhoneFlow phoneFlow = new PhoneFlow(upFlow, downFlow, sumFlow);
 context.write(new Text(s[0]),phoneFlow);

    }
}
