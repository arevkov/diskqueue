package ru.vi.diskqueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * User: arevkov
 * Date: 3/20/13
 * Time: 2:37 PM
 */
public class TestDiskQueue {

    public static final File storage = new File("./dqu_test");
    public static final String TEST_OBJECT = "2013-03-18 19:37:47,443 ID      SGu.tQuxFA5z5555BWpd    1       531351001363263012726000000000465498\n";
    public Serializer ser;

    @Before
    public void init() throws Exception {
        clean(storage);
        ser = new ProtobufSerializer(String.class, 2048);
    }

    @Test
    public void test1() throws Exception {
        // Test correctness
        DiskQueue<String> dq = new DiskQueue<String>(storage, ser, 2048);
        assert dq.offer(TEST_OBJECT);
        assert TEST_OBJECT.equals(dq.peek());
        assert dq.offer(TEST_OBJECT);
        assert TEST_OBJECT.equals(dq.peek());
        assert dq.peek() == null;
        assert dq.offer(TEST_OBJECT);
        assert dq.offer(TEST_OBJECT);
        assert TEST_OBJECT.equals(dq.peek());
        assert TEST_OBJECT.equals(dq.peek());
        assert dq.peek() == null;
        for (int i = 0; i < 200000; ++i) assert dq.offer(TEST_OBJECT);
        for (int i = 0; i < 200000; ++i) assert TEST_OBJECT.equals(dq.peek());
        assert dq.peek() == null;
        dq.shutdown();
    }

    @Test
    public void test2() throws Exception {
        DiskQueue<String> dq;// Test persistence
        dq = new DiskQueue<String>(storage, ser, 2048);
        assert dq.offer(TEST_OBJECT);
        assert dq.offer(TEST_OBJECT);
        assert dq.offer(TEST_OBJECT);
        dq.shutdown();
        dq = new DiskQueue<String>(storage, ser, 2048);
        assert TEST_OBJECT.equals(dq.peek());
        assert TEST_OBJECT.equals(dq.peek());
        assert TEST_OBJECT.equals(dq.peek());
        assert dq.peek() == null;
        dq.shutdown();
    }

    @After
    public void destroy() throws IOException {
        clean(storage);
    }

    public static void clean(File dir) throws IOException {
        if (!dir.exists()) return;
        System.gc();

        File[] files = dir.listFiles();
        if (files != null)
            for (File file : files) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    file.deleteOnExit();
                }
            }

        try {
            dir.delete();
        } catch (Exception e) {
            // do nothing - usual for Win
        }
    }
}
