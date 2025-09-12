/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.flink.runtime.io.network.netty;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkNotNull;

import com.huawei.omniruntime.flink.runtime.io.network.buffer.OmniLargeDataNetworkBuffer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nullable;

/**
 * BufferResponse message.
 *
 * @since 2025-04-23
 */
public class OmniBufferResponseDecoder extends NettyMessageDecoder {
    /**
     * The BufferResponse message that has its message header decoded, but still not received all
     * the bytes of the buffer part.
     */
    ByteBufAllocator nettyByteBufferAllocator;
    @Nullable
    private NettyMessage.BufferResponse bufferResponse;

    /**
     * How many bytes have been received or discarded for the data buffer part.
     */
    private int decodedDataBufferSize;

    /**
     * The Buffer allocator.
     */
    private final NetworkBufferAllocator allocator;

    /**
     * The accumulation buffer of message header.
     */
    private ByteBuf messageHeaderBuffer;

    OmniBufferResponseDecoder(NetworkBufferAllocator allocator) {
        this.allocator = checkNotNull(allocator);
    }

    @Override
    public void onChannelActive(ChannelHandlerContext ctx) {
        messageHeaderBuffer = ctx.alloc().directBuffer(MESSAGE_HEADER_LENGTH);
        nettyByteBufferAllocator = ctx.alloc();
    }

    /**
     * This method is called when a message is read from the channel.
     *
     * @param data The data read from the channel.
     * @return The decoding result.
     * @throws Exception If an error occurs during decoding.
     */
    @Override
    public DecodingResult onChannelRead(ByteBuf data) throws Exception {
        if (bufferResponse == null) {
            decodeMessageHeader(data);
            // for the case of received buffer is bigger than segment size
            if (bufferResponse != null) {
                int segmentSize = getSegmentSize(bufferResponse);
                if (bufferResponse.bufferSize > segmentSize) {
                    ByteBuf largeDataBuffer = nettyByteBufferAllocator.directBuffer(bufferResponse.bufferSize);
                    OmniLargeDataNetworkBuffer largeDataNetworkBuffer = new OmniLargeDataNetworkBuffer(largeDataBuffer,
                            bufferResponse.getBuffer(), bufferResponse.bufferSize);
                    bufferResponse.setLargeDataBuffer(largeDataBuffer);
                    bufferResponse.setBuffer(largeDataNetworkBuffer);
                }
            }
        }

        if (bufferResponse != null) {
            int remainingBufferSize = bufferResponse.bufferSize - decodedDataBufferSize;
            int actualBytesToDecode = Math.min(data.readableBytes(), remainingBufferSize);

            // For the case of data buffer really exists in BufferResponse now.
            if (actualBytesToDecode > 0) {
                // For the case of released input channel, the respective data buffer part would be
                // discarded from the received buffer.
                if (bufferResponse.getBuffer() == null) {
                    data.readerIndex(data.readerIndex() + actualBytesToDecode);
                } else {
                    if (bufferResponse.isLargeDataBuffer()) {
                        bufferResponse.getLargeDataBuffer().writeBytes(data, actualBytesToDecode);
                    } else {
                        bufferResponse.getBuffer().asByteBuf().writeBytes(data, actualBytesToDecode);
                    }
                }

                decodedDataBufferSize += actualBytesToDecode;
            }

            if (decodedDataBufferSize == bufferResponse.bufferSize) {
                NettyMessage.BufferResponse result = bufferResponse;
                clearState();
                return DecodingResult.fullMessage(result);
            }
        }

        return DecodingResult.NOT_FINISHED;
    }

    private void decodeMessageHeader(ByteBuf data) {
        ByteBuf fullFrameHeaderBuf =
                ByteBufUtils.accumulate(
                        messageHeaderBuffer,
                        data,
                        MESSAGE_HEADER_LENGTH,
                        messageHeaderBuffer.readableBytes());
        if (fullFrameHeaderBuf != null) {
            bufferResponse = NettyMessage.BufferResponse.readFrom(fullFrameHeaderBuf, allocator);
        }
    }

    private void clearState() {
        bufferResponse = null;
        decodedDataBufferSize = 0;

        messageHeaderBuffer.clear();
    }

    @Override
    public void close() {
        if (bufferResponse != null) {
            bufferResponse.releaseBuffer();
        }

        messageHeaderBuffer.release();
    }

    /**
     * Returns the size of the segment.
     *
     * @param bufferResponse The buffer response.
     * @return The size of the segment.
     */
    public int getSegmentSize(NettyMessage.BufferResponse bufferResponse) {
        return bufferResponse.getBuffer().getMemorySegment().size();
    }
}
