package ru.vi.diskqueue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * User: arevkov
 * Date: 3/19/13
 * Time: 2:08 PM
 */
class Utils {

    private static final String ERR_TRUNCATED_MESSAGE =
            "While parsing a protocol message, the input ended unexpectedly " +
                    "in the middle of a field.  This could mean either than the " +
                    "input has been truncated or that an embedded message " +
                    "misreported its own length.";

    public static int len(int varInt32) {
        if (varInt32 < 0) {
            throw new IllegalArgumentException(String.valueOf(varInt32));
        } else if (varInt32 < 0x80) {
            return 1;
        } else if (varInt32 < 0x4000) {
            return 2;
        } else if (varInt32 < 0x200000) {
            return 3;
        } else if (varInt32 < 0x10000000) {
            return 4;
        } else {
            return 5;
        }
    }

    /** Encode and write a varint to the {@link java.io.OutputStream} */
    public static void writeRawVarInt32(OutputStream out, int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                out.write(value);
                return;
            } else {
                out.write((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    public static void writeRawVarInt32(ByteBuffer bb, int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                bb.put((byte) value);
                return;
            } else {
                bb.put((byte) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }
    }

    /**
     * Reads a varint from the input one byte at a time, so that it does not
     * read any bytes after the end of the varint.  If you simply wrapped the
     * stream in a CodedInput and used {@link #readRawVarInt32(java.io.InputStream)}
     * then you would probably end up reading past the end of the varint since
     * CodedInput buffers its input.
     */
    public static int readRawVarInt32(final InputStream input) throws IOException {
        final int firstByte = input.read();
        if (firstByte == -1) {
            throw new EOFException("delimiter");
        }

        if ((firstByte & 0x80) == 0) {
            return firstByte;
        }
        return readRawVarint32(input, firstByte);
    }

    /**
     * Reads a varint from the input one byte at a time, so that it does not
     * read any bytes after the end of the varint.  If you simply wrapped the
     * stream in a CodedInput and used {@link #readRawVarInt32(java.io.InputStream)}
     * then you would probably end up reading past the end of the varint since
     * CodedInput buffers its input.
     */
    private static int readRawVarint32(final InputStream input, final int firstByte) throws IOException {
        int result = firstByte & 0x7f;
        int offset = 7;
        for (; offset < 32; offset += 7) {
            final int b = input.read();
            if (b == -1) {
                throw new EOFException("delimiter");
            }
            result |= (b & 0x7f) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        // Keep reading up to 64 bits.
        for (; offset < 64; offset += 7) {
            final int b = input.read();
            if (b == -1) {
                throw new EOFException("delimiter");
            }
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new RuntimeException(ERR_TRUNCATED_MESSAGE);
    }

    /**
     * Reads a varint from the input one byte at a time, so that it does not
     * read any bytes after the end of the varint.  If you simply wrapped the
     * stream in a CodedInput and used {@link #readRawVarInt32(java.io.InputStream)}
     * then you would probably end up reading past the end of the varint since
     * CodedInput buffers its input.
     */
    public static int readRawVarInt32(final ByteBuffer input) throws IOException {
        final int firstByte = input.get() & 0xff;
        if (firstByte == -1) {
            throw new EOFException("delimiter");
        }

        if ((firstByte & 0x80) == 0) {
            return firstByte;
        }
        return readRawVarint32(input, firstByte);
    }

    /**
     * Reads a varint from the input one byte at a time, so that it does not
     * read any bytes after the end of the varint.  If you simply wrapped the
     * stream in a CodedInput and used {@link #readRawVarInt32(java.io.InputStream)}
     * then you would probably end up reading past the end of the varint since
     * CodedInput buffers its input.
     */
    private static int readRawVarint32(final ByteBuffer input, final int firstByte) throws IOException {
        int result = firstByte & 0x7f;
        int offset = 7;
        for (; offset < 32; offset += 7) {
            final int b = input.get() & 0xff;
            if (b == -1) {
                throw new EOFException("delimiter");
            }
            result |= (b & 0x7f) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        // Keep reading up to 64 bits.
        for (; offset < 64; offset += 7) {
            final int b = input.get() & 0xff;
            if (b == -1) {
                throw new EOFException("delimiter");
            }
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new RuntimeException(ERR_TRUNCATED_MESSAGE);
    }
}
