package ru.vi.diskqueue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * User: arevkov
 * Date: 26.03.13
 * Time: 23:27
 */
class Meta {
    public final int headId;
    public final int tailId;
    public final int readPos;
    public final int writePos;

    Meta(int headId, int tailId, int readPos, int writePos) {
        this.headId = headId;
        this.tailId = tailId;
        this.readPos = readPos;
        this.writePos = writePos;
    }

    public static Meta read(File file) throws IOException {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            return new Meta(raf.readInt(),
                    raf.readInt(),
                    raf.readInt(),
                    raf.readInt());
        } finally {
            if (raf != null) raf.close();
        }
    }

    public static void write(File file, int headId, int tailId, int readPos, int writePos) throws IOException {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            raf.setLength(0);
            raf.writeInt(headId);
            raf.writeInt(tailId);
            raf.writeInt(readPos);
            raf.writeInt(writePos);
        } finally {
            if (raf != null) raf.close();
        }
    }
}
