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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class UnframedThriftMessageDecodeHelper
{
    private final int maxFrameSize;
    private final TProtocolFactory protocolFactory;

    public UnframedThriftMessageDecodeHelper(int maxFrameSize, TProtocolFactory protocolFactory)
    {
        checkArgument(maxFrameSize > 0, "Max frame size capped at 2 GB");
        this.maxFrameSize = maxFrameSize;
        this.protocolFactory = protocolFactory;
    }

    public ChannelBuffer tryDecodeUnframedMessage(ChannelHandlerContext ctx, Channel channel,
                                              ChannelBuffer buffer) throws TException
    {
        // Perform a trial decode, skipping through
        // the fields, to see whether we have an entire message available.

        int messageLength = 0;
        int messageStartReaderIndex = buffer.readerIndex();

        try {
            TNiftyTransport decodeAttemptTransport =
                    new TNiftyTransport(channel, buffer, ThriftTransportType.UNFRAMED);
            TProtocol inputProtocol = protocolFactory.getProtocol(decodeAttemptTransport);

            // Skip through the message
            inputProtocol.readMessageBegin();
            TProtocolUtil.skip(inputProtocol, TType.STRUCT);
            inputProtocol.readMessageEnd();

            messageLength = buffer.readerIndex() - messageStartReaderIndex;
        } catch (IndexOutOfBoundsException exception) {
            // No complete message was decoded: ran out of bytes
            return null;
        } finally {
            if (buffer.readerIndex() - messageStartReaderIndex > maxFrameSize) {
                Channels.fireExceptionCaught(
                        ctx,
                        new TooLongFrameException("Maximum frame size of " + maxFrameSize + " exceeded")
                );
            }

            buffer.readerIndex(messageStartReaderIndex);
        }

        checkState(messageLength >= 0);

        if (messageLength == 0) {
            // Caught an exception before decoding a full message
            return null;
        }

        // We have a full message in the buffer, slice it off
        ChannelBuffer messageBuffer = buffer.slice(messageStartReaderIndex, messageLength);
        buffer.readerIndex(messageStartReaderIndex + messageLength);
        return messageBuffer;
    }
}
