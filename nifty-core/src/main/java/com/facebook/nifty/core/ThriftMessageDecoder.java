/*
 * Copyright (C) 2012-2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.nifty.core;

import org.apache.thrift.protocol.TProtocolFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class ThriftMessageDecoder extends FrameDecoder {
    private final UnframedThriftMessageDecodeHelper unframedDecodeHelper;
    private final FramedThriftMessageDecodeHelper framedDecodeHelper;

    public ThriftMessageDecoder(int maxFrameSize, TProtocolFactory inputProtocolFactory) {
        unframedDecodeHelper = new UnframedThriftMessageDecodeHelper(maxFrameSize, inputProtocolFactory);
        framedDecodeHelper = new FramedThriftMessageDecodeHelper(maxFrameSize);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
            throws Exception {
        if (!buffer.readable()) {
            return null;
        }

        ChannelBuffer messageBuffer;

        messageBuffer = framedDecodeHelper.tryDecodeFramedMessage(ctx, channel, buffer, true);
        if (messageBuffer != null) {
            return new TNiftyTransport(channel, messageBuffer, ThriftTransportType.FRAMED);
        }

        messageBuffer = unframedDecodeHelper.tryDecodeUnframedMessage(ctx, channel, buffer);
        if (messageBuffer != null) {
            return new TNiftyTransport(channel, messageBuffer, ThriftTransportType.UNFRAMED);
        }

        return null;
    }
}
