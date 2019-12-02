import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PhoneFlow implements Writable {
    private int upFlow;
    private  int downFlow;
    private  int sumFlow;

    public PhoneFlow() {
    }

    public PhoneFlow(int upFlow, int downFlow, int sumFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    public int getUpflow() {
        return upFlow;
    }

    public void setUpflow(int upflow) {
        this.upFlow = upflow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(int sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
upFlow =dataInput.readInt();
downFlow =dataInput.readInt();
sumFlow =dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhoneFlow phoneFlow = (PhoneFlow) o;
        return upFlow == phoneFlow.upFlow &&
                downFlow == phoneFlow.downFlow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upFlow, downFlow);
    }

    @Override
    public String toString() {
        return  "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
