package ru.vi.diskqueue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * User: arevkov
 * Date: 18.03.13
 * Time: 23:25
 */
interface Segment {
    long size();

    void load(ByteBuffer bb) throws Exception;

    void store(ByteBuffer bb) throws Exception;

    void close() throws IOException;

    void delete() throws IOException;

    boolean isEmpty();

    int getReadPosition();

    int getWritePosition();

    File getFile();
}
