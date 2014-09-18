package ru.vi.diskqueue;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * User: arevkov
 * Date: 3/20/13
 * Time: 12:37 PM
 */
public class TestUtils {

    private static final int MAX_VALUE = 100000;

    @Test
    public void testVarIntByteBuffer() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        for (int i = 1; i < MAX_VALUE; ++i) {
            try {
                bb.clear();
                Utils.writeRawVarInt32(bb, i);
                bb.flip();
                int i0 = Utils.readRawVarInt32(bb);
                assert i == i0 : String.format("failed int: %d != %d", i, i0);
            } catch (Exception e) {
                throw new AssertionError(Integer.toString(i), e);
            }
        }
    }

    @Test
    public void testVarIntStreaming() throws IOException {
        for (int i = 1; i < MAX_VALUE; ++i) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(8);
                Utils.writeRawVarInt32(baos, i);
                int i0 = Utils.readRawVarInt32(new ByteArrayInputStream(baos.toByteArray()));
                assert i == i0 : String.format("failed int: %d != %d", i, i0);
            } catch (Exception e) {
                throw new AssertionError(Integer.toString(i), e);
            }
        }
    }

    @Test
    public void testVarIntLen() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        testVarIntLen0(bb, 0);
        for (int i = 1; i > 0 && i < Integer.MAX_VALUE; i *= 2) {
            testVarIntLen0(bb, i);
        }
        testVarIntLen0(bb, Integer.MAX_VALUE);
    }

    private void testVarIntLen0(ByteBuffer bb, int i) {
        try {
            bb.clear();
            Utils.writeRawVarInt32(bb, i);
            int len = Utils.len(i);
            assert bb.position() == len : String.format("Utils.len(%d) = %d, should be %d", i, len, bb.position());
            System.out.printf("VarInt32.len(0x%x)=%d\n", i, len);
        } catch (Exception e) {
            throw new AssertionError(Integer.toString(i), e);
        }
    }
}
