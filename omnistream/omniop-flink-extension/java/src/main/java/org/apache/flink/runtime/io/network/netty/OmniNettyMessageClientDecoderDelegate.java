/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.flink.runtime.io.network.netty;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.FRAME_HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.MAGIC_NUMBER;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delegate for decoding messages from the network. It is responsible for decoding the frame
 * header and delegating the decoding of the message body to the appropriate decoder.
 *
 * @since 2025-04-23
 */
public class OmniNettyMessageClientDecoderDelegate extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(OmniNettyMessageClientDecoderDelegate.class);

    /** The decoder for BufferResponse. */
    private final NettyMessageDecoder bufferResponseDecoder;

    /** The decoder for messages other than BufferResponse. */
    private final NettyMessageDecoder nonBufferResponseDecoder;

    /** The accumulation buffer for the frame header. */
    private ByteBuf frameHeaderBuffer;

    /** The decoder for the current message. It is null if we are decoding the frame header. */
    private NettyMessageDecoder currentDecoder;

    OmniNettyMessageClientDecoderDelegate(NetworkClientHandler networkClientHandler) {
        this.bufferResponseDecoder =
                new OmniBufferResponseDecoder(
                        new NetworkBufferAllocator(checkNotNull(networkClientHandler)));
        this.nonBufferResponseDecoder = new NonBufferResponseDecoder();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        bufferResponseDecoder.onChannelActive(ctx);
        nonBufferResponseDecoder.onChannelActive(ctx);
        frameHeaderBuffer = ctx.alloc().directBuffer(FRAME_HEADER_LENGTH);
        super.channelActive(ctx);
    }

    /**
     * Releases resources when the channel is closed. When exceptions are thrown during processing
     * received netty buffers, {@link CreditBasedPartitionRequestClientHandler} is expected to catch
     * the exception and close the channel and trigger this notification.
     *
     * @param ctx The context of the channel close notification.
     * @throws Exception Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        IOUtils.cleanup(LOG, bufferResponseDecoder, nonBufferResponseDecoder);
        frameHeaderBuffer.release();

        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf data = (ByteBuf) msg;
        try {
            while (data.isReadable()) {
                if (currentDecoder != null) {
                    NettyMessageDecoder.DecodingResult result = currentDecoder.onChannelRead(data);
                    if (!result.isFinished()) {
                        break;
                    }
                    ctx.fireChannelRead(result.getMessage());

                    currentDecoder = null;
                    frameHeaderBuffer.clear();
                }

                decodeFrameHeader(data);
            }
            checkState(!data.isReadable(), "Not all data of the received buffer consumed.");
        } finally {
            data.release();
        }
    }

    private void decodeFrameHeader(ByteBuf data) {
        ByteBuf fullFrameHeaderBuf =
                ByteBufUtils.accumulate(
                        frameHeaderBuffer,
                        data,
                        FRAME_HEADER_LENGTH,
                        frameHeaderBuffer.readableBytes());

        if (fullFrameHeaderBuf != null) {
            int messageAndFrameLength = fullFrameHeaderBuf.readInt();
            checkState(
                    messageAndFrameLength >= 0,
                    "The length field of current message must be non-negative");

            int magicNumber = fullFrameHeaderBuf.readInt();
            checkState(
                    magicNumber == MAGIC_NUMBER,
                    "Network stream corrupted: received incorrect magic number.");

            int msgId = fullFrameHeaderBuf.readByte();
            if (msgId == NettyMessage.BufferResponse.ID) {
                currentDecoder = bufferResponseDecoder;
            } else {
                currentDecoder = nonBufferResponseDecoder;
            }

            currentDecoder.onNewMessageReceived(msgId, messageAndFrameLength - FRAME_HEADER_LENGTH);
        }
    }
}
