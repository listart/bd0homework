package listart.hive.file.format.geek;

import listart.hive.file.format.geek.util.GeekUtil;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import java.io.IOException;
import java.util.Arrays;

public class GeekRecordWriter implements RecordWriter, JobConfigurable {
    RecordWriter writer;
    BytesWritable bytesWritable;

    public GeekRecordWriter(RecordWriter writer) {
        this.writer = writer;
        bytesWritable = new BytesWritable();
    }

    @Override
    public void write(Writable writable) throws IOException {
        // get input data
        byte[] input;
        int size;

        if (writable instanceof  Text) {
            Text text = (Text) writable;
            input = text.getBytes();
            size = text.getLength();
        } else {
            assert (writable instanceof  BytesWritable);
            BytesWritable bytesWritable = (BytesWritable) writable;
            input = bytesWritable.getBytes();
            size = bytesWritable.getLength();
        }

        // trim data
        if (input.length != size)
            input = Arrays.copyOf(input, size);

        // encode
        byte[] output = GeekUtil.encode(input);

        bytesWritable.set(output, 0, output.length);
        writer.write(bytesWritable);
    }

    @Override
    public void close(boolean abort) throws IOException {
        writer.close(abort);
    }

    @Override
    public void configure(JobConf job) {

    }
}
