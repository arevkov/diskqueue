package ru.vi.diskqueue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * User: arevkov
 * Date: 3/26/13
 * Time: 3:24 PM
 */
public class TestKill9 {

    private static final String TEST_OBJECT = "2013-03-18 19:37:47,443 ID      SGu.tQuxFA5z5555BWpd    1       531351001363263012726000000000465498\n";

    public static void main(String[] args) throws Exception {
        final DiskQueue<String> dq = new DiskQueue<String>(new File("./test_dqu"),
                new ProtobufSerializer<String>(String.class, 2048), 2048);
        if (dq.peek() == null) {
            for (int i = 0; ++i < 100000;) dq.offer(TEST_OBJECT);
        } else {
            while (dq.peek() != null);
        }
        if (Boolean.parseBoolean(args[0])) dq.shutdown();
    }

    @Test
    public void test() throws IOException, InterruptedException {
        int cnt = 10;
        while (--cnt >= 0) {
            String workingDir = TestKill9.class.getClassLoader().getResource("").getPath();
            String classes = new File(new File(workingDir).getParentFile(), "classes").getPath();
            String classpath = new File(new File(workingDir).getParent(), "dependency").getPath();

            Process proc = Runtime.getRuntime().exec(
                    String.format("java -ea -classpath %s/*:%s:%s TestKill9 %s", classpath, workingDir, classes, true),
                    new String[]{}, new File(workingDir));

            // Read process' stdout
            InputStream in = proc.getInputStream();
            int c;
            while ((c = in.read()) != -1) {
                System.out.print((char) c);
            }
            in.close();
            int code = proc.waitFor();

            // Detect exit code
            if (code != 0) {
                while ((c = proc.getErrorStream().read()) != -1)
                    System.out.print((char) c);
                throw new RuntimeException("exit_code: " + code);
            }
        }
    }
}
