/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http2;

import com.aayushatharva.brotli4j.decoder.DecoderJNI;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.util.internal.ObjectUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Decompresses a {@link ByteBuf} encoded with the brotli format while bounding the total decompressed output.
 * <p>
 * Used by {@link DelegatingDecompressorFrameListener} when a {@code maxAllocation} limit is configured. Decompressed
 * output is forwarded downstream as it is produced, in buffers of at most 64 KiB, and the limit is enforced inside
 * the decoder: before a decompressed buffer is allocated the cumulative output is checked, and once it would exceed
 * {@code maxAllocation} decompression stops, the native decoder is released and a {@link DecompressionException} is
 * thrown. Any further input is discarded.
 * <p>
 * This decoder only depends on API that is public in {@code netty-codec}, so the bound holds no matter how the
 * {@code netty-codec} {@link io.netty.handler.codec.compression.BrotliDecoder} on the classpath buffers its output.
 */
final class BoundedBrotliDecoder extends ByteToMessageDecoder {

    private static final int INPUT_BUFFER_SIZE = 8 * 1024;
    private static final int MAX_FORWARD_BYTES = 64 * 1024;

    private enum State {
        DONE, NEEDS_MORE_INPUT, ERROR
    }

    static {
        try {
            Brotli.ensureAvailability();
        } catch (Throwable throwable) {
            throw new ExceptionInInitializerError(throwable);
        }
    }

    private final int maxAllocation;
    private DecoderJNI.Wrapper decoder;
    private boolean destroyed;
    private boolean needsRead;
    private long decompressedBytes;

    /**
     * Creates a new instance.
     *
     * @param maxAllocation maximum number of decompressed bytes this decoder will produce. Must be &gt; 0.
     */
    BoundedBrotliDecoder(int maxAllocation) {
        this.maxAllocation = ObjectUtil.checkPositive(maxAllocation, "maxAllocation");
    }

    private void forwardOutput(ChannelHandlerContext ctx) {
        ByteBuffer nativeBuffer = decoder.pull();
        // nativeBuffer actually wraps brotli's internal buffer so we need to copy its content. A single pull may
        // return up to a whole brotli window, so it is copied and forwarded in bounded chunks and the limit is
        // checked before each chunk is allocated. Stop if the decoder was released while forwarding (for example
        // because the channel was closed by a downstream handler) as the native buffer is no longer valid then.
        while (nativeBuffer.hasRemaining() && !destroyed) {
            int chunkSize = Math.min(nativeBuffer.remaining(), MAX_FORWARD_BYTES);
            if (decompressedBytes + chunkSize > maxAllocation) {
                throw new DecompressionException("Decompression buffer has reached maximum size: " + maxAllocation);
            }
            ByteBuf copy = ctx.alloc().buffer(chunkSize);
            ByteBuffer chunk = nativeBuffer.slice();
            chunk.limit(chunkSize);
            copy.writeBytes(chunk);
            nativeBuffer.position(nativeBuffer.position() + chunkSize);
            decompressedBytes += chunkSize;
            needsRead = false;
            ctx.fireChannelRead(copy);
        }
    }

    private State decompress(ChannelHandlerContext ctx, ByteBuf input) {
        for (;;) {
            if (destroyed) {
                // Released while output was forwarded, nothing more can be decompressed.
                return State.DONE;
            }
            switch (decoder.getStatus()) {
                case DONE:
                    return State.DONE;

                case OK:
                    decoder.push(0);
                    break;

                case NEEDS_MORE_INPUT:
                    while (decoder.hasOutput() && !destroyed) {
                        forwardOutput(ctx);
                    }

                    if (destroyed || !input.isReadable()) {
                        return State.NEEDS_MORE_INPUT;
                    }

                    ByteBuffer decoderInputBuffer = decoder.getInputBuffer();
                    decoderInputBuffer.clear();
                    int readBytes = readBytes(input, decoderInputBuffer);
                    decoder.push(readBytes);
                    break;

                case NEEDS_MORE_OUTPUT:
                    forwardOutput(ctx);
                    break;

                default:
                    return State.ERROR;
            }
        }
    }

    private static int readBytes(ByteBuf in, ByteBuffer dest) {
        int limit = Math.min(in.readableBytes(), dest.remaining());
        ByteBuffer slice = dest.slice();
        slice.limit(limit);
        in.readBytes(slice);
        dest.position(dest.position() + limit);
        return limit;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        decoder = new DecoderJNI.Wrapper(INPUT_BUFFER_SIZE);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        needsRead = true;
        if (destroyed) {
            // Skip data received after finished or after the limit was exceeded.
            in.skipBytes(in.readableBytes());
            return;
        }

        if (!in.isReadable()) {
            return;
        }

        try {
            State state = decompress(ctx, in);
            if (state == State.DONE) {
                destroy();
            } else if (state == State.ERROR) {
                throw new DecompressionException("Brotli stream corrupted");
            }
        } catch (Exception e) {
            destroy();
            // Nothing more will be decompressed, discard the remaining input.
            in.skipBytes(in.readableBytes());
            throw e;
        }
    }

    private void destroy() {
        if (!destroyed) {
            destroyed = true;
            decoder.destroy();
        }
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        try {
            destroy();
        } finally {
            super.handlerRemoved0(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            destroy();
        } finally {
            super.channelInactive(ctx);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // Discard bytes of the cumulation buffer if needed.
        discardSomeReadBytes();

        if (needsRead && !ctx.channel().config().isAutoRead()) {
            ctx.read();
        }
        ctx.fireChannelReadComplete();
    }
}
