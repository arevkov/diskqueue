package ru.vi.diskqueue;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
* User: arevkov
* Date: 3/18/13
* Time: 6:00 PM
*/
public class ProtobufSerializer<V> implements Serializer<V> {
    /** */
    private final Schema<V> schema;
    /** */
    private final Class<V> classe;
    /** */
    private final ThreadLocal<ByteBuffer> encodeBuffers;

    private final Field offsetFld;

    public ProtobufSerializer(final String className, final int MAX_BUFFER_SIZE) throws ClassNotFoundException, NoSuchFieldException {
        this((Class<V>) Class.forName(className), MAX_BUFFER_SIZE);
    }

    public ProtobufSerializer(final Class<V> classe, final int MAX_BUFFER_SIZE) throws NoSuchFieldException {
        this.classe = classe;
        this.schema = RuntimeSchema.getSchema(classe);
        this.encodeBuffers = new ThreadLocal<ByteBuffer>(){
            @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocate(MAX_BUFFER_SIZE);
            }
        };
        this.offsetFld = LinkedBuffer.class.getDeclaredField("offset");
        this.offsetFld.setAccessible(true);
    }

    @Override
    public ByteBuffer encode(V v) throws Exception {
        ByteBuffer bb = encodeBuffers.get();
        bb.clear();
        LinkedBuffer lb = LinkedBuffer.use(bb.array());
        ProtostuffIOUtil.writeTo(lb, v, schema);
        bb.limit(offsetFld.getInt(lb));
        return bb;
    }

    @Override
    public V decode(ByteBuffer bb) throws Exception {
        V v = classe.newInstance();
        ProtostuffIOUtil.mergeFrom(bb.array(), 0, bb.limit(), v, schema);
        return v;
    }
}
