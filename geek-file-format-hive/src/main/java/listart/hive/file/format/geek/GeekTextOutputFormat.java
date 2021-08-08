package listart.hive.file.format.geek;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable> extends HiveIgnoreKeyTextOutputFormat<K, V> {
    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
                                                             Path outPath,
                                                             Class<? extends Writable> valueClass,
                                                             boolean isCompressed,
                                                             Properties tableProperties,
                                                             Progressable progress) throws IOException {
        GeekRecordWriter writer = new GeekRecordWriter(
                super.getHiveRecordWriter(jc, outPath, valueClass, isCompressed, tableProperties, progress));

        writer.configure(jc);

        return writer;
    }
}
