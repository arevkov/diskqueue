package ru.vi.diskqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: arevkov
 * Date: 3/18/13
 * Time: 4:26 PM
 */
public class DiskQueue<V> {
    public static final Logger log = LoggerFactory.getLogger(DiskQueue.class);

    private static final long MB = 1024L * 1024L;

    /** Directory to store all files */
    private final File dir;
    /** Serializer */
    private final Serializer<V> ser;
    /** Maximum size of single file, default 100MB */
    private final int segmentSize = (int) (50 * MB);
    /** Segments */
    private final Deque<Segment> segments;
    /** Max size, default 2GB */
    private final long capacity;
    /** Current size */
    private final AtomicLong size;
    /** */
    private final ThreadLocal<ByteBuffer> readBuffers;

    /** Locks */
    private final FileLock lock;
    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();

    /** Flag means that storage has been shutdown */
    private volatile boolean shutdown;
    /** Last index */
    private volatile int idx;
    /** */
    private volatile boolean full;

    public DiskQueue(File dir, Serializer<V> ser, final int maxBufferSize) throws IOException {
        this(dir, ser, 2048, maxBufferSize);
    }

    public DiskQueue(File dir, Serializer ser, int capacityMB, final int maxBufferSize) throws IOException {
        this.capacity = MB * capacityMB;
        if (capacity < 2 * segmentSize) throw new RuntimeException("Too small capacity, as minimum: "
                + (2 * segmentSize / MB) + "MB");

        this.writeLock.lock();
        this.readLock.lock();

        FileLock lock = null;
        try {
            lock = acquireLock(dir);
            this.dir = dir;
            this.ser = ser;
            this.readBuffers = new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocate(maxBufferSize);
                }
            };
            this.segments = Loader.load(dir, segmentSize, false);
            this.idx = Loader.getSegmentIdx(segments.getLast().getFile());
            this.size = new AtomicLong();
            for (Segment seg : segments)
                size.addAndGet(seg.getFile().exists() ? seg.getFile().length() : segmentSize);
            log.info("start: size={}MB, segments={}, head={}, read_pos={}, tail={}, write_pos={}",
                    new Object[]{size.get() / MB,
                            segments.size(),
                            segments.getFirst().getFile().getName(),
                            segments.getFirst().getReadPosition(),
                            segments.getLast().getFile().getName(),
                            segments.getLast().getWritePosition()});
        } catch (Exception e) {
            if (lock != null) {
                try {
                    lock.release();
                    lock.channel().close();
                } catch (Exception e0) { /* do nothing */ }
            }
            throw new RuntimeException("Failed to initialize storage: " + dir.getCanonicalPath(), e);
        } finally {
            this.lock = lock;
            this.writeLock.unlock();
            this.readLock.unlock();
        }
    }

    private FileLock acquireLock(File dir) throws IOException {
        if (!dir.exists()) dir.mkdirs();
        // Acquire lock:
        File lockFile = new File(dir, "lock");
        FileLock lock = new RandomAccessFile(lockFile, "rw").getChannel().tryLock();
        if (lock == null) throw new RuntimeException(
                "Another process holds lock to file: " + lockFile.getCanonicalPath());
        return lock;
    }

    /**
     * Add at the end
     *
     * @param v
     * @return false if full
     * @throws Exception
     */
    public boolean offer(V v) throws Exception {
        if (v == null) throw new NullPointerException("value");
        if (full) return false;
        if (log.isTraceEnabled()) log.trace("WRITE\t{}", v);
        // Serialize before sync block!!!!
        final ByteBuffer bb = ser.encode(v);
        if (bb.limit() == 0) throw new IllegalArgumentException("serializer returned empty byte[]");
        try {
            // Acquire lock to write
            writeLock.lock();
            if (shutdown) throw new RuntimeException("terminated");
            // Push tail ahead if size exceeded limits
            for (;;) {
                try {
                    segments.getLast().store(bb);
                    return true;
                } catch (BufferOverflowException e) {
                    // If total size exceeded - return false
                    if (size.get() + segmentSize > capacity) {
                        log.info("full: {}MB", size.get() / MB);
                        full = true;
                        return false;
                    }
                    shift();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Retrieve first element and remove
     *
     * @return null if empty
     * @throws Exception
     */
    public V peek() throws Exception {
        // Will be decoded after sync block
        final ByteBuffer bb = readBuffers.get();
        bb.clear();
        try {
            // Acquire lock to read
            readLock.lock();
            if (shutdown) throw new RuntimeException("terminated");
            retry_loop:
            for (;;) {
                Segment head = segments.getFirst();
                if (segments.size() == 1) {
                    // Head and tail is the same
                    if (head.isEmpty()) return null;
                    else head.load(bb);
                    break retry_loop;
                } else {
                    try {
                        head.load(bb);
                        break retry_loop; // Done
                    } catch (EOFException e) {
                        // Segment has been completely read
                        segments.pollFirst();
                        size.addAndGet(-head.size());
                        head.delete();
                        full = false;
                        if (log.isDebugEnabled())
                            log.debug("delete({}) {}", head.getFile().exists() ? "fail" : "success", head.getFile().getName());
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
        // De-serialize strictly after releasing read lock!!!!
        bb.flip();
        V v = ser.decode(bb);
        if (log.isTraceEnabled()) log.trace("READ\t{}", v);
        return v;
    }

    public void shutdown() throws IOException {
        try {
            writeLock.lock();
            readLock.lock();
            if (shutdown) return;
            // Write last write position
            int readPos = segments.getFirst().getReadPosition();
            int writePos = segments.getLast().getWritePosition();
            Meta.write(new File(dir, "meta"),
                    Loader.getSegmentIdx(segments.getFirst().getFile()),
                    Loader.getSegmentIdx(segments.getLast().getFile()),
                    readPos, writePos);
            // Save exception if any
            Exception e = null;
            for (Segment segment : segments) {
                try { segment.close(); } catch (IOException e1) { e = e1; }
            }
            log.info("terminate: size={}MB, segments={}, head={}, read_pos={}, tail={}, write_pos={}",
                    new Object[]{size.get() / 1024 / 1024,
                            segments.size(),
                            segments.getFirst().getFile().getName(),
                            segments.getFirst().getReadPosition(),
                            segments.getLast().getFile().getName(),
                            segments.getLast().getWritePosition()});
            // Release FileLock
            lock.release();
            lock.channel().close();
            shutdown = true;
            if (e != null) throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
            readLock.unlock();
        }
    }

    public boolean isEmpty() {
        return segments.size() == 1 && segments.getFirst().isEmpty();
    }

    private void shift() throws IOException {
        try {
            // ReadLock is needed to be ensure that ReadWrite node has never been read!
            readLock.lock();
            final Segment tail = segments.getLast();
            if (log.isDebugEnabled()) log.debug("shift: {}", tail.getFile());
            if (tail.getReadPosition() == 0) {
                // Convert to ReadOnly node(friendly memory usage)
                tail.close();
                segments.removeLast();
                segments.addLast(new ReadOnly(tail.getFile(), 0));
            }
            segments.add(new ReadWrite(new File(dir, Loader.getFileName(++idx)), 0, 0, segmentSize));
            size.addAndGet(segmentSize);
        } finally {
            readLock.unlock();
        }
    }
}
