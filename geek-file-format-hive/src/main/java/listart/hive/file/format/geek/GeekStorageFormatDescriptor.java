package listart.hive.file.format.geek;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;

import java.util.Set;

public class GeekStorageFormatDescriptor extends AbstractStorageFormatDescriptor {
    public static final String GEEK = "GEEK";

    public static final Set<String> GEEK_NAMES = ImmutableSet.of(GEEK);
    public static final String GEEKFILE_INPUT = GeekTextInputFormat.class.getName();
    public static final String GEEKFILE_OUTPUT = GeekTextOutputFormat.class.getName();

    @Override
    public Set<String> getNames() {
        return GEEK_NAMES;
    }

    @Override
    public String getInputFormat() {
        return GEEKFILE_INPUT;
    }

    @Override
    public String getOutputFormat() {
        return GEEKFILE_OUTPUT;
    }
}
