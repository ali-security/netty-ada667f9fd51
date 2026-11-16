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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.DecompressionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Exploit-style tests for the streaming decoders {@link DelegatingDecompressorFrameListener} uses when a
 * {@code maxAllocation} limit is configured (CVE-2026-42587): the decoders are driven directly, with a downstream
 * handler counting every byte they forward, to prove a decompression bomb is rejected before it is fully expanded.
 */
public abstract class AbstractBoundedDecoderTest {
    private static final int MAX_ALLOCATION = 64 * 1024;
    private static final int MAX_FORWARD_BYTES = 64 * 1024;

    protected abstract boolean isCodecAvailable();

    protected abstract ChannelHandler newDecoder(int maxAllocation);

    protected abstract byte[] compress(byte[] data) throws Exception;

    @BeforeEach
    public void assumeCodecAvailable() {
        assumeTrue(isCodecAvailable(), "compression codec is not available on this platform");
    }

    @Test
    public void testBombInSingleBufferRejectedBeforeFullExpansion() throws Exception {
        // 4 MiB of zeros compresses to a few bytes: a single inbound buffer that expands far beyond the limit.
        byte[] bomb = compress(new byte[4 * 1024 * 1024]);
        CountingHandler counter = new CountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(newDecoder(MAX_ALLOCATION), counter);
        try {
            Throwable cause = writeInbound(channel, Unpooled.wrappedBuffer(bomb));
            assertMaxAllocationExceeded(cause);
            assertTrue(counter.bytes <= MAX_ALLOCATION,
                    "forwarded " + counter.bytes + " bytes, must be bounded by maxAllocation " + MAX_ALLOCATION);
            assertTrue(counter.maxChunk <= MAX_FORWARD_BYTES,
                    "forwarded a buffer of " + counter.maxChunk + " bytes, must be at most " + MAX_FORWARD_BYTES);

            // Once the limit was exceeded any further input is discarded.
            long forwarded = counter.bytes;
            assertNull(writeInbound(channel, Unpooled.wrappedBuffer(bomb)));
            assertEquals(forwarded, counter.bytes);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testBombSpreadOverManyBuffersRejected() throws Exception {
        // The limit is cumulative: feeding the compressed content in small pieces must not get around it.
        byte[] compressed = compress(compressibleData(1024 * 1024));
        CountingHandler counter = new CountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(newDecoder(MAX_ALLOCATION), counter);
        try {
            Throwable cause = null;
            int offset = 0;
            while (offset < compressed.length && cause == null) {
                int length = Math.min(1024, compressed.length - offset);
                cause = writeInbound(channel, Unpooled.wrappedBuffer(compressed, offset, length));
                offset += length;
            }
            assertMaxAllocationExceeded(cause);
            assertTrue(counter.bytes <= MAX_ALLOCATION,
                    "forwarded " + counter.bytes + " bytes, must be bounded by maxAllocation " + MAX_ALLOCATION);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testContentUpToLimitDecompressed() throws Exception {
        byte[] data = compressibleData(MAX_ALLOCATION);
        CountingHandler counter = new CountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(newDecoder(MAX_ALLOCATION), counter);
        try {
            assertNull(writeInbound(channel, Unpooled.wrappedBuffer(compress(data))));
            assertArrayEquals(data, counter.out.toByteArray());
            assertTrue(counter.maxChunk <= MAX_FORWARD_BYTES);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testSmallLimit() throws Exception {
        int maxAllocation = 1000;
        byte[] data = compressibleData(maxAllocation);
        CountingHandler counter = new CountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(newDecoder(maxAllocation), counter);
        try {
            assertNull(writeInbound(channel, Unpooled.wrappedBuffer(compress(data))));
            assertArrayEquals(data, counter.out.toByteArray());
        } finally {
            channel.finishAndReleaseAll();
        }

        counter = new CountingHandler();
        channel = new EmbeddedChannel(newDecoder(maxAllocation), counter);
        try {
            byte[] tooLarge = compress(compressibleData(maxAllocation + 1));
            Throwable cause = writeInbound(channel, Unpooled.wrappedBuffer(tooLarge));
            assertMaxAllocationExceeded(cause);
            assertTrue(counter.bytes <= maxAllocation,
                    "forwarded " + counter.bytes + " bytes, must be bounded by maxAllocation " + maxAllocation);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static Throwable writeInbound(EmbeddedChannel channel, ByteBuf buf) {
        try {
            channel.writeInbound(buf);
            return null;
        } catch (Throwable cause) {
            return cause;
        }
    }

    private static void assertMaxAllocationExceeded(Throwable cause) {
        assertNotNull(cause, "decompression must be rejected once maxAllocation is exceeded");
        Throwable t = cause;
        while (t != null && !(t instanceof DecompressionException)) {
            t = t.getCause();
        }
        assertNotNull(t, "expected a DecompressionException in the cause chain of " + cause);
        assertTrue(String.valueOf(t.getMessage()).contains("maximum size"), "unexpected message: " + t.getMessage());
    }

    private static byte[] compressibleData(int length) {
        Random random = new Random(42);
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = i % 4 != 0 ? 0 : (byte) random.nextInt();
        }
        return data;
    }

    private static final class CountingHandler extends ChannelInboundHandlerAdapter {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        long bytes;
        int maxChunk;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buf = (ByteBuf) msg;
            try {
                int readable = buf.readableBytes();
                bytes += readable;
                maxChunk = Math.max(maxChunk, readable);
                buf.readBytes(out, readable);
            } finally {
                buf.release();
            }
        }
    }
}
