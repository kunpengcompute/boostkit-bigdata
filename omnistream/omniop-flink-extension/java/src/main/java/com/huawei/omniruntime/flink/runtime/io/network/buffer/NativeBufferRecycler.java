package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NativeBufferRecycler
 *
 * @version 1.0.0
 * @since 2025/03/14
 */

public class NativeBufferRecycler implements BufferRecycler {
    private static final Logger LOG = LoggerFactory.getLogger(NativeBufferRecycler.class);
    private static final ConcurrentHashMap<Long, NativeBufferRecycler> INSTANCE_MAP = new ConcurrentHashMap<>();

    private Map<MemorySegment, Long> memorySegmentToAddress = new HashMap<>();
    private long nativeCreditBasedSequenceNumberingViewReaderRef;

    private NativeBufferRecycler(long nativeCreditBasedSequenceNumberingViewReaderRef) {
        this.nativeCreditBasedSequenceNumberingViewReaderRef = nativeCreditBasedSequenceNumberingViewReaderRef;
    }

    /**
     * getInstance
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     * @return NativeBufferRecycler
     */
    public static synchronized NativeBufferRecycler getInstance(long nativeCreditBasedSequenceNumberingViewReaderRef) {
        NativeBufferRecycler bufferRecycler = INSTANCE_MAP.get(nativeCreditBasedSequenceNumberingViewReaderRef);
        if (bufferRecycler == null) {
            bufferRecycler = new NativeBufferRecycler(nativeCreditBasedSequenceNumberingViewReaderRef);
            INSTANCE_MAP.put(nativeCreditBasedSequenceNumberingViewReaderRef, bufferRecycler);
        }
        return bufferRecycler;
    }


    @Override
    public void recycle(MemorySegment memorySegment) {
        Long address = memorySegmentToAddress.remove(memorySegment);
        if (address != null) {
            freeNativeByteBuffer(nativeCreditBasedSequenceNumberingViewReaderRef, address);
        }
    }

    /**
     * registerMemorySegment
     *
     * @param memorySegment memorySegment
     * @param address address
     */
    public void registerMemorySegment(MemorySegment memorySegment, long address) {
        memorySegmentToAddress.put(memorySegment, address);
    }

    /**
     * unRegisterInstance
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     */
    public static void unRegisterInstance(long nativeCreditBasedSequenceNumberingViewReaderRef) {
        NativeBufferRecycler nativeBufferRecycler =
                INSTANCE_MAP.remove(nativeCreditBasedSequenceNumberingViewReaderRef);
        if (nativeBufferRecycler != null) {
            LOG.info("**** Unregister native buffer recycler instance and find there are {} memory need to be freed.",
                    nativeBufferRecycler.memorySegmentToAddress.size());
            for (Map.Entry<MemorySegment, Long> entry : nativeBufferRecycler.memorySegmentToAddress.entrySet()) {
                nativeBufferRecycler.freeNativeByteBuffer(nativeCreditBasedSequenceNumberingViewReaderRef,
                        entry.getValue());
            }
            nativeBufferRecycler.memorySegmentToAddress.clear();
        }
    }


    /**
     * freeNativeByteBuffer
     *
     * @param address address
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     */
    public native void freeNativeByteBuffer(long nativeCreditBasedSequenceNumberingViewReaderRef, long address);
}
