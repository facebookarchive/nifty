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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

import static com.google.common.base.Preconditions.checkArgument;

public class FramedThriftMessageDecodeHelper
{
    private final static int MESSAGE_FRAME_SIZE = 4;
    private final int maxFrameSize;

    public FramedThriftMessageDecodeHelper(int maxFrameSize)
    {
        checkArgument(maxFrameSize > 0, "Max frame size is capped at 2GB and must be positive");
        this.maxFrameSize = maxFrameSize;
    }

    public ChannelBuffer tryDecodeFramedMessage(ChannelHandlerContext ctx,
                                                Channel channel,
                                                ChannelBuffer buffer,
                                                boolean stripFrameSize)
    {
        if (!checkMessageType(buffer)) {
            return null;
        }

        // Framed messages are prefixed by the size of the frame (which doesn't include the
        // framing itself).

        int messageStartReaderIndex = buffer.readerIndex();
        // Read the i32 frame contents size
        int messageContentsLength = buffer.getInt(messageStartReaderIndex);
        // The full message is larger by the size of the frame size prefix
        int messageLength = messageContentsLength + MESSAGE_FRAME_SIZE;

        if (messageContentsLength > maxFrameSize) {
            Channels.fireExceptionCaught(
                    ctx,
                    new TooLongFrameException("Maximum frame size of " + maxFrameSize + " exceeded")
            );
        }

        int messageContentsOffset = messageStartReaderIndex + MESSAGE_FRAME_SIZE;
        if (messageLength == 0) {
            // Zero-sized frame: just ignore it and return nothing
            buffer.readerIndex(messageContentsOffset);
            return null;
        }
        else if (buffer.readableBytes() < messageLength) {
            // Full message isn't available yet, return nothing for now
            return null;
        }
        else if (stripFrameSize) {
            // Full message is available, return it without the frame size
            ChannelBuffer messageBuffer = buffer.slice(messageContentsOffset, messageContentsLength);
            buffer.readerIndex(messageStartReaderIndex + messageLength);
            return messageBuffer;
        } else {
            // Full message is available, return it including the frame size
            ChannelBuffer messageBuffer = buffer.slice(messageStartReaderIndex, messageContentsOffset + messageContentsLength);
            buffer.readerIndex(messageStartReaderIndex + messageLength);
            return messageBuffer;
        }
    }

    protected boolean checkMessageType(ChannelBuffer buffer)
    {
        if (buffer.readableBytes() <= MESSAGE_FRAME_SIZE) {
            return false;
        }

        int frameSize = buffer.getInt(0);

        if ((frameSize & 0x80000000) != 0) {
            // Upper bit set, not a framed message
            return false;
        }

        return true;
    }
}
