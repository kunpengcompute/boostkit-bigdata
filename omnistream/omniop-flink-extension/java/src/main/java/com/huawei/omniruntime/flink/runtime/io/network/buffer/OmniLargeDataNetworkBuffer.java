/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;

/**
 * The class is used to handle large data network buffer.
 * It wraps the original network buffer and provides a way to access the large data buffer.
 *
 * @since 2025-04-23
 */
public class OmniLargeDataNetworkBuffer implements Buffer {
    DataType dataType;
    private ByteBuf bigDataBuffer;
    private Buffer originalBuffer;
    private int largeBufferSize;
    private MemorySegment memorySegment;


    public OmniLargeDataNetworkBuffer(ByteBuf byteBuf, Buffer networkBuffer, int largeBufferSize) {
        this.bigDataBuffer = byteBuf;
        this.originalBuffer = networkBuffer;
        this.largeBufferSize = largeBufferSize;
        this.memorySegment = MemorySegmentFactory.wrapOffHeapMemory(byteBuf.nioBuffer());
        dataType = DataType.DATA_BUFFER;
    }

    public ByteBuf getBigDataBuffer() {
        return bigDataBuffer;
    }

    public void setBigDataBuffer(ByteBuf bigDataBuffer) {
        this.bigDataBuffer = bigDataBuffer;
    }

    public Buffer getNetworkBuffer() {
        return originalBuffer;
    }
    public void setOriginalBuffer(Buffer buffer) {
        this.originalBuffer = buffer;
    }

    @Override
    public boolean isBuffer() {
        return true;
    }

    @Override
    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

    @Override
    public int getMemorySegmentOffset() {
        return 0;
    }

    @Override
    public BufferRecycler getRecycler() {
        return null;
    }

    @Override
    public void recycleBuffer() {
        if (bigDataBuffer != null) {
            bigDataBuffer.release();
            bigDataBuffer = null;
        }
        if (originalBuffer != null) {
            originalBuffer.recycleBuffer();
            originalBuffer = null;
        }
    }

    @Override
    public boolean isRecycled() {
        return false;
    }

    @Override
    public Buffer retainBuffer() {
        return null;
    }

    @Override
    public Buffer readOnlySlice() {
        return null;
    }

    @Override
    public Buffer readOnlySlice(int index, int length) {
        return null;
    }

    @Override
    public int getMaxCapacity() {
        return 0;
    }

    @Override
    public int getReaderIndex() {
        return 0;
    }

    @Override
    public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {

    }

    @Override
    public int getSize() {
        return largeBufferSize;
    }

    @Override
    public void setSize(int writerIndex) {

    }

    @Override
    public int readableBytes() {
        return 0;
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
        return null;
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
        return null;
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {

    }

    @Override
    public ByteBuf asByteBuf() {
        return null;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public void setCompressed(boolean isCompressed) {

    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public int refCnt() {
        return 0;
    }
}
