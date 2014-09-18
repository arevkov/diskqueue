package ru.vi.diskqueue;

import java.nio.ByteBuffer;

/**
 * Implementation should be thread safe!!!
 *
 * User: arevkov
 * Date: 3/18/13
 * Time: 4:39 PM
 */
public interface Serializer<V> {

    ByteBuffer encode(V v) throws Exception;

    V decode(ByteBuffer bb) throws Exception;
}
