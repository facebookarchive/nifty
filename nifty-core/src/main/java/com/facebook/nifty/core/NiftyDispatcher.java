/**
 * Copyright 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.nifty.core;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Dispatch TNiftyTransport to the TProcessor and write output back.
 *
 * Note that all current async thrift clients are capable of sending multiple requests at once
 * but not capable of handling out-of-order responses to those requests, so this dispatcher
 * sends the requests in order. (Eventually this will be conditional on a flag in the thrift
 * message header for future async clients that can handle out-of-order responses).
 */
public class NiftyDispatcher extends SimpleChannelUpstreamHandler
{
    private static final Logger log = LoggerFactory.getLogger(NiftyDispatcher.class);

    private final TProcessorFactory processorFactory;
    private final TProtocolFactory inProtocolFactory;
    private final TProtocolFactory outProtocolFactory;
    private final Executor exe;
    private int dispatcherSequenceId = 0;
    private int lastResponseWrittenId = 0;
    private final Map<Integer, ChannelBuffer> responseMap = new HashMap<>();

    public NiftyDispatcher(ThriftServerDef def)
    {
        this.processorFactory = def.getProcessorFactory();
        this.inProtocolFactory = def.getInProtocolFactory();
        this.outProtocolFactory = def.getOutProtocolFactory();
        this.exe = def.getExecutor();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e)
            throws Exception
    {
        if (e.getMessage() instanceof TNiftyTransport) {
            TNiftyTransport messageTransport = (TNiftyTransport) e.getMessage();
            processRequest(ctx, messageTransport);
        }
        else {
            ctx.sendUpstream(e);
        }
    }

    private void processRequest(final ChannelHandlerContext ctx, final TNiftyTransport messageTransport) {
        // Remember the ordering of requests as they arrive, used to enforce an order on the
        // responses.
        final int responseSequenceId = ++dispatcherSequenceId;
        exe.execute(new Runnable()
        {
            @Override
            public void run()
            {
                TProtocol inProtocol = inProtocolFactory.getProtocol(messageTransport);
                TProtocol outProtocol = outProtocolFactory.getProtocol(messageTransport);
                try {
                    processorFactory.getProcessor(messageTransport).process(
                            inProtocol,
                            outProtocol
                    );
                    // Ensure responses to requests are written in the same order the requests
                    // were received.
                    synchronized (responseMap) {
                        ChannelBuffer response = messageTransport.getOutputBuffer();
                        int currentResponseId = lastResponseWrittenId + 1;
                        if (responseSequenceId != currentResponseId) {
                            // This response is NOT next in line of ordered responses, save it to
                            // be sent later, after responses to all earlier requests have been
                            // sent.
                            responseMap.put(responseSequenceId, response);
                        } else {
                            // This response was next in line, write this response now, and see if
                            // there are others next in line that should be sent now as well.
                            do {
                                Channels.write(ctx.getChannel(), response);
                                ++lastResponseWrittenId;
                                ++currentResponseId;
                            } while (null != (response = responseMap.remove(currentResponseId)));
                        }
                    }
                }
                catch (TException e1) {
                    log.error("Exception while invoking!", e1);
                    closeChannel(ctx);
                }
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception
    {
        // Any out of band exception are caught here and we tear down the socket
        closeChannel(ctx);
    }

    private void closeChannel(ChannelHandlerContext ctx)
    {
        if (ctx.getChannel().isOpen()) {
            ctx.getChannel().close();
        }
    }
}
