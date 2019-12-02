import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String string=value.toString();
        String[] s = string.split(" ");
        for (String s1 : s) {
            context.write(new Text(s1),new IntWritable(1));
        }
    }
}
