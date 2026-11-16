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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.BrotliDecoder;
import io.netty.handler.codec.compression.Zstd;
import io.netty.handler.codec.compression.ZstdDecoder;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class DelegatingDecompressorFrameListenerTest {

    private EmbeddedChannel parent;
    private ChannelHandlerContext ctx;

    @BeforeEach
    public void setUp() {
        parent = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        ctx = parent.pipeline().firstContext();
    }

    @AfterEach
    public void tearDown() {
        parent.finishAndReleaseAll();
    }

    @Test
    public void testBrotliLimitEnforcedByCodecHttp2Decoder() throws Exception {
        assumeTrue(Brotli.isAvailable());
        // With a limit the bound must not depend on how the netty-codec BrotliDecoder buffers its output.
        EmbeddedChannel decompressor = newListener(1024).newContentDecompressor(ctx, HttpHeaderValues.BR);
        try {
            assertNotNull(decompressor.pipeline().get(BoundedBrotliDecoder.class));
            assertNull(decompressor.pipeline().get(BrotliDecoder.class));
        } finally {
            decompressor.finishAndReleaseAll();
        }

        // Without a limit the default decoder is kept.
        decompressor = newListener(0).newContentDecompressor(ctx, HttpHeaderValues.BR);
        try {
            assertNotNull(decompressor.pipeline().get(BrotliDecoder.class));
            assertNull(decompressor.pipeline().get(BoundedBrotliDecoder.class));
        } finally {
            decompressor.finishAndReleaseAll();
        }
    }

    @Test
    public void testZstdLimitEnforcedByCodecHttp2Decoder() throws Exception {
        assumeTrue(Zstd.isAvailable());
        // With a limit the bound must not depend on how the netty-codec ZstdDecoder buffers its output.
        EmbeddedChannel decompressor = newListener(1024).newContentDecompressor(ctx, HttpHeaderValues.ZSTD);
        try {
            assertNotNull(decompressor.pipeline().get(BoundedZstdDecoder.class));
            assertNull(decompressor.pipeline().get(ZstdDecoder.class));
        } finally {
            decompressor.finishAndReleaseAll();
        }

        // Without a limit the default decoder is kept.
        decompressor = newListener(0).newContentDecompressor(ctx, HttpHeaderValues.ZSTD);
        try {
            assertNotNull(decompressor.pipeline().get(ZstdDecoder.class));
            assertNull(decompressor.pipeline().get(BoundedZstdDecoder.class));
        } finally {
            decompressor.finishAndReleaseAll();
        }
    }

    @Test
    public void testNegativeMaxAllocationRejected() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                newListener(-1);
            }
        });
    }

    private static DelegatingDecompressorFrameListener newListener(int maxAllocation) {
        return new DelegatingDecompressorFrameListener(new DefaultHttp2Connection(true), new Http2FrameAdapter(),
                maxAllocation);
    }
}
