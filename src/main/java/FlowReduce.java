import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text,PhoneFlow,Text,PhoneFlow> {
    @Override
    protected void reduce(Text key, Iterable<PhoneFlow> values, Context context) throws IOException, InterruptedException {
        int upFlow=0;
        int downFlow=0;
        for (PhoneFlow value : values) {
            upFlow = value.getUpflow();
            downFlow= value.getDownFlow();
        }

        PhoneFlow phoneFlow = new PhoneFlow(upFlow, downFlow, upFlow + downFlow);
        context.write(key,phoneFlow);
    }
}
