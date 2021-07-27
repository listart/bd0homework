package listart;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public FlowBean() {
    }

    public FlowBean(Long upFlow, Long downFlow) {
        set(upFlow, downFlow);
    }

    public void set(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    public void reset() {
        set(0L, 0L);
    }

    public void add(FlowBean value) {
        set(upFlow + value.upFlow, downFlow + value.downFlow);
    }
}
