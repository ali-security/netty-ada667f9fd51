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

import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.Zstd;
import io.netty.util.internal.ObjectUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Decompresses a {@link ByteBuf} encoded with the Zstandard format while bounding the total decompressed output.
 * <p>
 * Used by {@link DelegatingDecompressorFrameListener} when a {@code maxAllocation} limit is configured. Decompressed
 * output is produced into buffers of at most {@code min(maxAllocation, 64 KiB)} bytes which are forwarded downstream
 * as soon as they are filled, and the limit is enforced inside the decoder: once the cumulative output would exceed
 * {@code maxAllocation} decompression stops and a {@link DecompressionException} is thrown. Any further input is
 * discarded.
 * <p>
 * This decoder only depends on API that is public in {@code netty-codec}, so the bound holds no matter how the
 * {@code netty-codec} {@link io.netty.handler.codec.compression.ZstdDecoder} on the classpath buffers its output.
 */
final class BoundedZstdDecoder extends ByteToMessageDecoder {
    private static final int MAX_FORWARD_BYTES = 64 * 1024;

    // Don't use static here as we want to still allow to load the classes.
    {
        try {
            Zstd.ensureAvailability();
        } catch (Throwable throwable) {
            throw new ExceptionInInitializerError(throwable);
        }
    }

    private final int maxAllocation;
    private final int outputBufferSize;
    private final MutableByteBufInputStream inputStream = new MutableByteBufInputStream();
    private ZstdInputStreamNoFinalizer zstdIs;

    private boolean needsRead;
    private boolean closed;
    private long decompressedBytes;
    private State currentState = State.DECOMPRESS_DATA;

    /**
     * Current state of stream.
     */
    private enum State {
        DECOMPRESS_DATA,
        CORRUPTED
    }

    /**
     * Creates a new instance.
     *
     * @param maxAllocation maximum number of decompressed bytes this decoder will produce. Must be &gt; 0.
     */
    BoundedZstdDecoder(int maxAllocation) {
        this.maxAllocation = ObjectUtil.checkPositive(maxAllocation, "maxAllocation");
        outputBufferSize = Math.min(maxAllocation, MAX_FORWARD_BYTES);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        needsRead = true;
        try {
            if (currentState == State.CORRUPTED || closed) {
                in.skipBytes(in.readableBytes());
                return;
            }
            inputStream.current = in;

            ByteBuf outBuffer = null;
            try {
                int w;
                do {
                    if (outBuffer == null) {
                        outBuffer = ctx.alloc().heapBuffer(outputBufferSize);
                    }
                    do {
                        w = outBuffer.writeBytes(zstdIs, outBuffer.writableBytes());
                    } while (w != -1 && outBuffer.isWritable());
                    if (outBuffer.isReadable()) {
                        int readable = outBuffer.readableBytes();
                        if (decompressedBytes + readable > maxAllocation) {
                            throw new DecompressionException(
                                    "Decompression buffer has reached maximum size: " + maxAllocation);
                        }
                        decompressedBytes += readable;
                        ByteBuf decompressed = outBuffer;
                        outBuffer = null;
                        needsRead = false;
                        ctx.fireChannelRead(decompressed);
                    }
                    // Stop if the decoder was removed while forwarding (for example because the channel was closed
                    // by a downstream handler) as the zstd stream is released then.
                } while (w != -1 && !closed);
            } finally {
                if (outBuffer != null) {
                    outBuffer.release();
                }
            }
        } catch (DecompressionException e) {
            currentState = State.CORRUPTED;
            in.skipBytes(in.readableBytes());
            throw e;
        } catch (Exception e) {
            currentState = State.CORRUPTED;
            in.skipBytes(in.readableBytes());
            throw new DecompressionException(e);
        } finally {
            inputStream.current = null;
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

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        zstdIs = new ZstdInputStreamNoFinalizer(inputStream);
        zstdIs.setContinuous(true);
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        try {
            closed = true;
            closeSilently(zstdIs);
        } finally {
            super.handlerRemoved0(ctx);
        }
    }

    private static void closeSilently(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {
                // ignore
            }
        }
    }

    private static final class MutableByteBufInputStream extends InputStream {
        ByteBuf current;

        @Override
        public int read() {
            if (current == null || !current.isReadable()) {
                return -1;
            }
            return current.readByte() & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            int available = available();
            if (available == 0) {
                return -1;
            }

            len = Math.min(available, len);
            current.readBytes(b, off, len);
            return len;
        }

        @Override
        public int available() {
            return current == null ? 0 : current.readableBytes();
        }
    }
}
