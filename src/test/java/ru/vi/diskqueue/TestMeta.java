package ru.vi.diskqueue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * User: arevkov
 * Date: 3/27/13
 * Time: 1:29 PM
 */
public class TestMeta {

    @Test
    public void test() throws IOException {
        final File file = new File("meta");
        try {
            testMeta(file, 0, 0, 0, 0);
            testMeta(file, 1, 1, 1, 1);
            testMeta(file, 2, 2, 2, 2);
        } finally {
            file.delete();
        }
    }

    public void testMeta(final File file,
                         final int headId,
                         final int tailId,
                         final int readPos,
                         final int writePos) throws IOException {
        Meta.write(file, headId, tailId, readPos, writePos);
        Meta meta = Meta.read(file);
        assert meta.headId == headId;
        assert meta.tailId == tailId;
        assert meta.readPos == readPos;
        assert meta.writePos == writePos;
    }
}
