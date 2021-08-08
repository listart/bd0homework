package listart.hive.file.format.geek;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class GeekTextInputFormat implements InputFormat<LongWritable, BytesWritable>, JobConfigurable {
    TextInputFormat format;
    JobConf job;

    public GeekTextInputFormat() {
        this.format = new TextInputFormat();
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split,
                                                                     JobConf job,
                                                                     Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());

        GeekLineRecordReader reader = new GeekLineRecordReader(
                new LineRecordReader(job, (FileSplit) split));

        reader.configure(job);

        return reader;
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }
}
