package ru.vi.diskqueue;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: arevkov
 * Date: 3/19/13
 * Time: 2:30 PM
 */
public class Benchmark {

    private static String DATA = "2013-03-18 19:37:47,443 ID      SGu.tQuxFA5z5555BWpd    1       531351001363263012726000000000465498\n";
    private static final AtomicInteger writes = new AtomicInteger();
    private static final AtomicInteger reads = new AtomicInteger();
    private static final DiskQueue<String> queue;

    private static final int READ_THREADS = 4;
    private static final int WRITE_THREADS = 1;

    static {
        try {
            queue = new DiskQueue<String>(new File("./dqu"), new ProtobufSerializer<String>(String.class, 1024), 2048, 1024);
//            queue = new DiskQueue<String>(new File("./dqu"), new UTF8Serializer(), 1024);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        for (int i =0; i < READ_THREADS; ++i)   new Thread(new Read()).start();
        for (int i =0; i < WRITE_THREADS; ++i)  new Thread(new Write()).start();

        Runtime.getRuntime().addShutdownHook(new Thread(new SafeRunnable() {
            @Override
            protected void runImpl() throws Exception {
                System.out.println("shutdown-hook");
                queue.shutdown();
            }
        }));

        for (; ; ) {
            int w1 = writes.getAndSet(0);
            int r1 = reads.getAndSet(0);
            System.out.printf("%d w/s, %s r/s\n", w1, r1);
            Thread.sleep(1000);
        }
    }

    public static class Read extends SafeRunnable {

        @Override
        public void runImpl() throws Exception {
            while (true) {
                String row;
                while ((row = queue.peek()) == null) Thread.sleep(20);
                if (!DATA.equals(row)) throw new RuntimeException(String.format("\"%s\"", row));
                reads.incrementAndGet();
            }
        }
    }

    public static class Write extends SafeRunnable {

        @Override
        public void runImpl() throws Exception {
            while (true) {
                while (!queue.offer(DATA)) Thread.sleep(20);
                writes.incrementAndGet();
            }
        }
    }

    public static abstract class SafeRunnable implements Runnable {

        @Override
        public final void run() {
            try {
                runImpl();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected abstract void runImpl() throws Exception;
    }
}
