package ru.vi.diskqueue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * User: arevkov
 * Date: 26.03.13
 * Time: 23:25
 */
class Loader {

    static final String EXT = ".dqu";

    static String getFileName(int idx) {
        return String.format("%07d%s", idx, EXT);
    }

    static Deque<Segment> load(File dir, int segmentSize, boolean failfast) throws IOException {
        // Load all segments from directory
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                return name.endsWith(EXT);
            }
        });
        // Read meta information
        Meta meta = readMeta(new File(dir, "meta"));
        // Load
        return load(dir, files, meta, segmentSize, failfast);
    }

    static Deque<Segment> load(File dir, File[] files, Meta meta, int segmentSize, boolean failfast) throws IOException {
        Deque<Segment> segments = new LinkedList<Segment>();
        // Sort all files by order
        TreeMap<Integer, File> tree = new TreeMap<Integer, File>();
        if (files != null)
            for (File file : files) {
                try {
                    int idx = getSegmentIdx(file);
                    if (idx > 0) {
                        if (DiskQueue.log.isDebugEnabled())
                            DiskQueue.log.debug("loaded {}", file.getName());
                        tree.put(idx, file);
                    } else {
                        DiskQueue.log.warn("non-positive file index {}", file.getName());
                    }
                } catch (NumberFormatException e) {
                    DiskQueue.log.warn("invalid file name: {}", file.getName());
                }
            }
        // some validation for consistency
        Integer prev = null;
        for (Integer idx : tree.navigableKeySet()) {
            if (prev != null && idx != prev + 1)
                exception("missed files: " + getFileName(prev + 1) + " - " + getFileName(idx - 1), failfast);
            prev = idx;
        }
        // Check consistency
        int readPos = 0, writePos = 0;
        if (meta != null) {
            readPos = meta.readPos;
            writePos = meta.writePos;
            if (meta.readPos > 0 && (tree.isEmpty() || tree.firstKey() != meta.headId)) {
                exception("invalid head: " + getFileName(meta.headId), failfast);
                readPos = 0;
            }
            if (meta.writePos > 0 && (tree.isEmpty() || tree.lastKey() != meta.tailId)) {
                exception("invalid tail: " + getFileName(meta.tailId), failfast);
                writePos = 0;
            }
        }
        // Normalize file names to 00000001.dqu
        tree = normalizeSegNames(tree);
        int idx = tree.isEmpty() ? 0 : tree.lastKey();
        // Add to queue by order
        Map.Entry<Integer, File> next;
        while ((next = tree.pollFirstEntry()) != null) {
            if (!tree.isEmpty()) {
                // Head or body
                segments.add(new ReadOnly(next.getValue(), readPos));
            } else {
                // Add tail
                if (writePos > 0) {
                    segments.add(new ReadWrite(next.getValue(), readPos, writePos, segmentSize));
                } else {
                    // by some reason we lost meta info, so:
                    segments.add(new ReadOnly(next.getValue(), readPos));
                    segments.add(new ReadWrite(new File(dir, getFileName(++idx)), 0, 0, segmentSize));
                }
            }
            // Only head segment could have read position greater than zero
            readPos = 0;
        }
        // Set new tail if queue is empty
        if (segments.isEmpty()) {
            segments.add(new ReadWrite(new File(dir, getFileName(++idx)), 0, 0, segmentSize));
        }
        return segments;
    }

    static int getSegmentIdx(File file) {
        return Integer.valueOf(file.getName()
                .substring(0, file.getName().indexOf('.')));
    }

    private static void exception(String msg, boolean failfast) {
        if (failfast) throw new RuntimeException(msg);
        else DiskQueue.log.warn(msg);
    }

    private static Meta readMeta(File file) throws IOException {
        try {
            if (!file.exists()) return null;
            return Meta.read(file);
        } catch (Exception e) {
            System.err.printf("Failed read meta: %s\n", e.toString());
            return null;
        } finally {
            // Clean all current meta
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(0);
            raf.close();
        }
    }

    private static TreeMap<Integer, File> normalizeSegNames(TreeMap<Integer, File> tree) throws IOException {
        if (tree.isEmpty()) return tree;
        final int diff = tree.firstKey() - 1;
        if (diff == 0) return tree;
        // Renaming process(may be terminated, so acquire lock):
        TreeMap<Integer, File> renamed = new TreeMap<Integer, File>();
        for (Map.Entry<Integer, File> e; (e = tree.pollFirstEntry()) != null; ) {
            int idx = e.getKey() - diff;
            renamed.put(idx, rename(e.getValue(), idx));
        }
        return renamed;
    }

    private static File rename(File src, int idx) throws IOException {
        File dest = new File(src.getParent(), getFileName(idx));
        if (src.exists() && !src.renameTo(dest))
            throw new RuntimeException("Failed to rename: " + src.getCanonicalPath());
        if (DiskQueue.log.isDebugEnabled())
            DiskQueue.log.debug("rename {} -> {}", src.getName(), dest.getName());
        return dest;
    }
}
