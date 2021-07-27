package listart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean bean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        bean.reset();

        for (FlowBean value : values) {
            bean.add(value);
        }

        context.write(key, bean);
    }
}
