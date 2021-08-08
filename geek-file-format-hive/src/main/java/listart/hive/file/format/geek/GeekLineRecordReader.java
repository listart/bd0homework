package listart.hive.file.format.geek;

import listart.hive.file.format.geek.util.GeekUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Arrays;

public class GeekLineRecordReader implements RecordReader<LongWritable, BytesWritable>, JobConfigurable {
    LineRecordReader reader;
    Text text;

    public GeekLineRecordReader(LineRecordReader reader) {
        this.reader = reader;
        text = reader.createValue();
    }

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public boolean next(LongWritable key, BytesWritable value) throws IOException {
        while (reader.next(key, text)) {
            // text -> byte[] -> value
            byte[] textBytes = text.getBytes();
            int length = text.getLength();

            // Trim additional bytes
            if (length != textBytes.length) {
                textBytes = Arrays.copyOf(textBytes, length);
            }
            byte[] binaryData = GeekUtil.decode(textBytes);

            value.set(binaryData, 0, binaryData.length);
            return true;
        }

        // no more data
        return false;
    }

    @Override
    public LongWritable createKey() {
        return reader.createKey();
    }

    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

    @Override
    public long getPos() throws IOException {
        return reader.getPos();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }
}
