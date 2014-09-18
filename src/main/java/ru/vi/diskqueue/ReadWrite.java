package ru.vi.diskqueue;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
* Not thread-safe
*
* User: arevkov
* Date: 3/18/13
* Time: 4:38 PM
*/
class ReadWrite implements Segment {
    public static final byte MAX_DELIM_SIZE = (byte) Utils.len(Integer.MAX_VALUE);

    /** File with data */
    private final File file;
    /** */
    private ByteBuffer writeBB;
    /** */
    private ByteBuffer readBB;
    /** */
    private RandomAccessFile raf;

    private final long size;

    private volatile int writePosition;

    private volatile int readPosition;

    ReadWrite(File file, int readPos, int writePos, int maxSize) throws IOException {
        this.file = file;
        this.size = file.exists() ? file.length() : maxSize;
        this.writePosition = writePos;
        this.readPosition = readPos;
    }

    private void init() throws IOException {
        this.raf = new RandomAccessFile(file, "rw");
        this.writeBB = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
        this.writeBB.position(writePosition);
        this.readBB = writeBB.asReadOnlyBuffer();
        this.readBB.position(readPosition);
        if (DiskQueue.log.isDebugEnabled())
            DiskQueue.log.debug("{} read_pos={} write_pos={}",
                    new Object[]{file.getName(), readPosition, writePosition});
    }

    public File getFile() {
        return file;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void load(ByteBuffer bb) throws Exception {
        if (isEmpty()) throw new EOFException(); // TODO: consider friendly result
        if (readBB == null) init();
        int len = Utils.readRawVarInt32(readBB);
        if (len == 0) throw new EOFException();
        while (--len >= 0) {
            try {
                bb.put(readBB.get());
            } catch (BufferOverflowException e) {
                throw new IllegalStateException("unexpected journey");
            }
        }
        readPosition = readBB.position();
    }

    @Override
    public void store(ByteBuffer bb) throws Exception {
        bb.rewind();
        if (writeBB == null) init();
        if (bb.remaining() + MAX_DELIM_SIZE > writeBB.remaining())
            throw new BufferOverflowException();
        Utils.writeRawVarInt32(writeBB, bb.limit());
        writeBB.put(bb);
        writePosition = writeBB.position();
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.getChannel().force(true);
            raf.close();
        }
    }

    @Override
    public void delete() throws IOException {
        close();
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            // for Windows:
            file.deleteOnExit();
        }
    }

    public int getReadPosition() {
        return readPosition;
    }

    public int getWritePosition() {
        return writePosition;
    }

    public boolean isEmpty() {
        return readPosition == writePosition;
    }
}
