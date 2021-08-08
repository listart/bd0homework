package listart.hive.file.format.geek.util;

import java.util.*;

public class GeekUtil {
    public static byte[] decode(byte[] encodedBytes) {
        String encoded = new String(encodedBytes);
        String result = encoded.replaceAll("\\sge{2,256}k", "");

        return result.getBytes();
    }

    public static byte[] encode(byte[] rawBytes, Random rnd) {
        String [] tokens = new String(rawBytes).split("\\s");
        Queue<String> tokenQueue = new LinkedList<>(Arrays.asList(tokens));
        List<String> encodedTokens = new LinkedList<>();

        while (tokenQueue.size() > 0) {
            int batchSize = Math.min(rnd.nextInt(255) + 2, tokenQueue.size());

            for (int i = 0; i < batchSize; i++)
                encodedTokens.add(tokenQueue.remove());

            char[] token = new char[batchSize + 2];
            Arrays.fill(token, 'e');
            token[0] = 'g';
            token[token.length - 1] = 'k';

            encodedTokens.add(new String(token));
        }

        return String.join(" ", encodedTokens).getBytes();
    }

    public static byte[] encode(byte[] rawBytes, long seed) {
        return encode(rawBytes, new Random(seed));
    }

    public static byte[] encode(byte[] rawBytes) {
        return encode(rawBytes, new Random());
    }
}
