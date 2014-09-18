package ru.vi.diskqueue;

import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
 * User: arevkov
 * Date: 21.03.13
 * Time: 22:32
 */
public class TestFileDeleteWin7 {

    @Test
    public void test() throws IOException {
        final File file = new File("./to_delete.dqu");
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        final FileChannel ch = raf.getChannel();

        MappedByteBuffer buffer = ch.map(FileChannel.MapMode.READ_WRITE, 0, 1000);
        buffer.put((byte) 1);

        // Now target to release all resources and DELETE the file!!!
        raf.close();
        ch.close();

        // First solution(unsafe):
        sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        cleaner.clean();

        // Second solution(now way!!!):
//        buffer = null;
//        System.gc();

        // Third solution(lazy delete):
//        protected void finalize() {

        // Firth solution???
        // track: http://bugs.sun.com/view_bug.do?bug_id=4724038

        Files.delete(file.toPath());
    }
}
