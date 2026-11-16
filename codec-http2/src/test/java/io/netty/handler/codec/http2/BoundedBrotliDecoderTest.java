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

import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.compression.Brotli;

import java.io.ByteArrayOutputStream;

public class BoundedBrotliDecoderTest extends AbstractBoundedDecoderTest {

    @Override
    protected boolean isCodecAvailable() {
        return Brotli.isAvailable();
    }

    @Override
    protected ChannelHandler newDecoder(int maxAllocation) {
        return new BoundedBrotliDecoder(maxAllocation);
    }

    @Override
    protected byte[] compress(byte[] data) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        BrotliOutputStream brotliOs = new BrotliOutputStream(os);
        brotliOs.write(data);
        brotliOs.close();
        return os.toByteArray();
    }
}
