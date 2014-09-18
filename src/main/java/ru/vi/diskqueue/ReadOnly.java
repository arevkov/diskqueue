package ru.vi.diskqueue;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;

/**
* Unsafe
*
* User: arevkov
* Date: 3/18/13
* Time: 4:38 PM
*/
class ReadOnly implements Segment {
    /** File with data */
    private final File file;
    /** Size of segment */
    private final long size;
    /** lazy init. */
    private BufferedInputStream in;

    private int readPosition;

    ReadOnly(File file, int readPosition) throws IOException {
        this.file = file;
        this.size = file.exists() ? file.length() : 0;
        this.readPosition = readPosition;
    }

    private void init() throws IOException {
        in = new BufferedInputStream(new FileInputStream(file));
        readPosition = (int) in.skip(readPosition);
        if (DiskQueue.log.isDebugEnabled())
            DiskQueue.log.debug("{} read_pos={} write_pos=(read_only)",
                    file.getName(), readPosition);
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
        if (in == null) init();
        int len = Utils.readRawVarInt32(in);
        if (len == 0) throw new EOFException();
        byte[] arr = new byte[len];
        try { in.read(arr); } catch (EOFException e) { throw new IllegalStateException("unexpected journey"); }
        bb.put(arr);
        readPosition += len + Utils.len(len);
    }

    @Override
    public void store(ByteBuffer bb) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (in != null) in.close();
        in = null;
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

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getReadPosition() {
        return readPosition;
    }

    @Override
    public int getWritePosition() {
        throw new UnsupportedOperationException();
    }
}
