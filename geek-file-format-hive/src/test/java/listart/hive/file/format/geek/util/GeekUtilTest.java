package listart.hive.file.format.geek.util;

import org.junit.Test;
import static org.junit.Assert.*;

public class GeekUtilTest {
    public static final String RAW_DATA = "This notebook can be used to install gek on all worker nodes, run data generation, and create the TPCDS database.";
    public static final String ENCODED_DATA = "This notebook can be used geeeeek to install gek on all worker nodes, run data generation, and create the TPCDS database. geeeeeeeeeeeeeeek";

    @Test
    public void testDecode() {
        byte [] rawData = RAW_DATA.getBytes();
        byte [] encodedData = ENCODED_DATA.getBytes();
        byte [] decodedData = GeekUtil.decode(encodedData);

        assertArrayEquals(rawData, decodedData);
    }

    @Test
    public void testEncode() {
        long seed = 10;
        byte [] rawData = RAW_DATA.getBytes();
        byte [] encodedData = GeekUtil.encode(rawData, seed);

        assertArrayEquals(ENCODED_DATA.getBytes(), encodedData);
    }

}
