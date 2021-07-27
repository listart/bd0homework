package listart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, FlowBean> {
    private static final int FIELDS_SIZE = 11;

    private static final int INDEX_PHONE_NUMBER = 1;
    private static final int INDEX_UP_FLOW = 8;
    private static final int INDEX_DOWN_FLOW = 9;

    private Logger logger = Logger.getLogger(TokenizerMapper.class);

    private Text phone = new Text();
    private FlowBean bean = new FlowBean();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length == FIELDS_SIZE) {
            phone.set(fields[INDEX_PHONE_NUMBER]);
            bean.set(Long.parseLong(fields[INDEX_UP_FLOW]), Long.parseLong(fields[INDEX_DOWN_FLOW]));

            context.write(phone, bean);
        } else {
            logger.warn("ignore bad format data：" + value.toString());
        }
    }
}
