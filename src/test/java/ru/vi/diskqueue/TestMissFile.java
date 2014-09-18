package ru.vi.diskqueue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * User: arevkov
 * Date: 3/27/13
 * Time: 11:06 AM
 */
public class TestMissFile {

    private static String DATA = "2013-03-18 19:37:47,443 ID      SGu.tQuxFA5z5555BWpd    1       531351001363263012726000000000465498\n";

    private final Random rnd = new Random();

    @Test
    public void test() throws Exception {
        final File storage = new File("./test_dqu");

        // Fill initially
        DiskQueue<String> dq = buildDQ(storage);
        for (int i = 1000000; --i >= 0;) {
            dq.offer(DATA);
        }
        dq.shutdown();

        // Delete random file, then start -> peek -> offer
        int repeats = 1000;
        while (--repeats >= 0) {
            deleteRandom(storage);
            dq = buildDQ(storage);
            dq.peek();
            dq.offer(DATA);
            dq.shutdown();
        }
    }

    private void deleteRandom(File storage) {
        File[] files = storage.listFiles();
        if (files != null && files.length > 0) {
            File toDelete = files[rnd.nextInt(files.length - 1)];
            assert toDelete.delete();
            System.out.printf("delete: %s\n", toDelete.getName());
        }
    }

    private DiskQueue<String> buildDQ(File storage) throws IOException, NoSuchFieldException {
        return new DiskQueue<String>(storage,
                new ProtobufSerializer<String>(String.class, 2048), 2048);
    }
}
