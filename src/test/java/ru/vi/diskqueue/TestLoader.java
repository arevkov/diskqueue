package ru.vi.diskqueue;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Deque;

/**
 * User: arevkov
 * Date: 3/27/13
 * Time: 1:27 PM
 */
public class TestLoader {

    public static final File dir = new File("./test_dqu");

    @Test
    public void testEmpty() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[0],
                new Meta(1, 1, 0, 0), 1, false);

        assert seg.size() == 1;
        Segment e = seg.pollFirst();
        assert e.getReadPosition() == 0;
        assert e.getWritePosition() == 0;
        assert Loader.getSegmentIdx(e.getFile()) == 1;
    }

    @Test
    public void testSingle() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{new File(dir, "7.dqu")},
                new Meta(7, 7, 200, 400), 1, false);

        assert seg.size() == 1;
        Segment single = seg.pollFirst();
        assert single.getReadPosition() == 200;
        assert single.getWritePosition() == 400;
        assert Loader.getSegmentIdx(single.getFile()) == 1;
    }

    @Test
    public void testMissBody() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{new File(dir, "5.dqu"), new File(dir, "11.dqu")},
                new Meta(5, 11, 10, 10), 1, false);

        assert seg.size() == 2;

        Segment head = seg.pollFirst();
        assert head.getReadPosition() == 10;
        assert Loader.getSegmentIdx(head.getFile()) == 5 - 4;

        Segment tail = seg.pollFirst();
        assert tail.getReadPosition() == 0;
        assert tail.getWritePosition() == 10;
        assert Loader.getSegmentIdx(tail.getFile()) == 11 - 4;
    }

    @Test
    public void testMissHead() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{new File(dir, "10.dqu"), new File(dir, "11.dqu")},
                new Meta(5, 11, 10, 10), 1, false);

        assert seg.size() == 2;

        Segment newHead = seg.pollFirst();
        assert newHead.getReadPosition() == 0;
        assert Loader.getSegmentIdx(newHead.getFile()) == 10 - 9;

        Segment tail = seg.pollFirst();
        assert tail.getReadPosition() == 0;
        assert tail.getWritePosition() == 10;
        assert Loader.getSegmentIdx(tail.getFile()) == 11 - 9;
    }

    @Test
    public void testMissTail() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{new File(dir, "5.dqu"), new File(dir, "6.dqu")},
                new Meta(5, 7, 10, 10), 1, false);

        assert seg.size() == 3;

        Segment head = seg.pollFirst();
        assert head.getReadPosition() == 10;
        assert Loader.getSegmentIdx(head.getFile()) == 5 - 4;

        Segment body = seg.pollFirst();
        assert body.getReadPosition() == 0;
        assert Loader.getSegmentIdx(body.getFile()) == 6 - 4;

        Segment newTail = seg.pollFirst();
        assert newTail.getReadPosition() == 0;
        assert newTail.getWritePosition() == 0;
        assert Loader.getSegmentIdx(newTail.getFile()) == 7 - 4;
    }

    @Test
    public void testMissMeta() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{new File(dir, "5.dqu"), new File(dir, "6.dqu")},
                null, 1, false);

        assert seg.size() == 3;

        Segment head = seg.pollFirst();
        assert head.getReadPosition() == 0;
        assert Loader.getSegmentIdx(head.getFile()) == 5 - 4;

        Segment body = seg.pollFirst();
        assert body.getReadPosition() == 0;
        assert Loader.getSegmentIdx(body.getFile()) == 6 - 4;

        Segment newTail = seg.pollFirst();
        assert newTail.getReadPosition() == 0;
        assert newTail.getWritePosition() == 0;
        assert Loader.getSegmentIdx(newTail.getFile()) == 7 - 4;
    }

    @Test
    public void testMissAll() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[0],
                new Meta(8, 10, 5, 10), 1, false);

        assert seg.size() == 1;
        Segment e = seg.pollFirst();
        assert e.getReadPosition() == 0;
        assert e.getWritePosition() == 0;
        assert Loader.getSegmentIdx(e.getFile()) == 1;
    }

    @Test
    public void testInvalidHeadTail() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{new File(dir, "5.dqu"), new File(dir, "8.dqu")},
                new Meta(3, 10, 5, 10), 1, false);

        assert seg.size() == 3;

        Segment head = seg.pollFirst();
        assert head.getReadPosition() == 0;
        assert Loader.getSegmentIdx(head.getFile()) == 5 - 4;

        Segment body = seg.pollFirst();
        assert body.getReadPosition() == 0;
        assert Loader.getSegmentIdx(body.getFile()) == 8 - 4;

        Segment newTail = seg.pollFirst();
        assert newTail.getReadPosition() == 0;
        assert newTail.getWritePosition() == 0;
        assert Loader.getSegmentIdx(newTail.getFile()) == 9 - 4;
    }

    @Test
    public void testInvalidHeadTail2() throws IOException {
        Deque<Segment> seg = Loader.load(dir,
                new File[]{
                        new File(dir, "2.dqu"),
                        new File(dir, "3.dqu"),
                        new File(dir, "4.dqu"),
                        new File(dir, "5.dqu"),
                        new File(dir, "6.dqu"),
                        new File(dir, "7.dqu")
                },
                new Meta(2, 2, 5, 10), 1, false);

        assert seg.size() == 7;

        Segment head = seg.pollFirst();
        assert head.getReadPosition() == 5;
        assert Loader.getSegmentIdx(head.getFile()) == 2 - 1;

        for (int i = 2; i <= 6; ++i) {
            Segment body = seg.pollFirst();
            assert body.getReadPosition() == 0;
            assert Loader.getSegmentIdx(body.getFile()) == i;
        }

        Segment newTail = seg.pollFirst();
        assert newTail.getReadPosition() == 0;
        assert newTail.getWritePosition() == 0;
        assert Loader.getSegmentIdx(newTail.getFile()) == 7;
    }

    @Test
    public void testInvalidHeadTail3() throws IOException, NoSuchFieldException {
        try {
            TestDiskQueue.clean(dir);
            dir.mkdirs();

            File[] files = {
                    new File(dir, "2.dqu"),
                    new File(dir, "3.dqu"),
                    new File(dir, "4.dqu"),
                    new File(dir, "5.dqu"),
                    new File(dir, "6.dqu"),
                    new File(dir, "7.dqu")
            };
            Meta.write(new File(dir, "meta"), 2, 2, 5, 10);

            for (File file : files) file.createNewFile();

            DiskQueue dq = new DiskQueue(dir, new ProtobufSerializer<String>(String.class, 1024), 1024);
            dq.shutdown();
        } finally {
            TestDiskQueue.clean(dir);
        }
    }

    @After
    public void destroy() {
        try { TestDiskQueue.clean(dir); } catch (IOException e) { /* do nothing */ }
    }
}
