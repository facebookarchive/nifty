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
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A core channel the decode framed Thrift message, dispatches to the TProcessor given
 * and then encode message back to Thrift frame.
 */
public class NettyServerTransport implements ExternalResourceReleasable
{
    private static final Logger log = LoggerFactory.getLogger(NettyServerTransport.class);

    private final int port;
    private final ChannelPipelineFactory pipelineFactory;
    private static final int NO_WRITER_IDLE_TIMEOUT = 0;
    private static final int NO_ALL_IDLE_TIMEOUT = 0;
    private ServerBootstrap bootstrap;
    private Channel serverChannel;
    private final ThriftServerDef def;
    private final NettyServerConfig nettyServerConfig;

    @Inject
    public NettyServerTransport(
            final ThriftServerDef def,
            final NettyServerConfig nettyServerConfig,
            final ChannelGroup allChannels)
    {
        this.def = def;
        this.nettyServerConfig = nettyServerConfig;
        this.port = def.getServerPort();
        // connectionLimiter must be instantiated exactly once (and thus outside the pipeline factory)
        final ConnectionLimiter connectionLimiter = new ConnectionLimiter(def.getMaxConnections());

        this.pipelineFactory = new ChannelPipelineFactory()
        {
            @Override
            public ChannelPipeline getPipeline()
                    throws Exception
            {
                ChannelPipeline cp = Channels.pipeline();
                TProtocolFactory inputProtocolFactory = def.getDuplexProtocolFactory().getInputProtocolFactory();
                cp.addLast("connectionLimiter", connectionLimiter);
                cp.addLast(ChannelStatistics.NAME, new ChannelStatistics(allChannels));
                cp.addLast("frameCodec", def.getThriftFrameCodecFactory().create(def.getMaxFrameSize(),
                                                                                 inputProtocolFactory));
                if (def.getClientIdleTimeout() != null) {
                    // Add handlers to detect idle client connections and disconnect them
                    cp.addLast("idleTimeoutHandler", new IdleStateHandler(nettyServerConfig.getTimer(),
                                                                          (int) def.getClientIdleTimeout().toMillis(),
                                                                          NO_WRITER_IDLE_TIMEOUT,
                                                                          NO_ALL_IDLE_TIMEOUT,
                                                                          TimeUnit.MILLISECONDS));
                    cp.addLast("idleDisconnectHandler", new IdleDisconnectHandler());
                }

                cp.addLast("dispatcher", new NiftyDispatcher(def));
                cp.addLast("exceptionLogger", new NiftyExceptionLogger());
                return cp;
            }
        };
    }

    public void start()
    {
        ExecutorService bossExecutor = nettyServerConfig.getBossExecutor();
        int bossThreadCount = nettyServerConfig.getBossThreadCount();
        ExecutorService workerExecutor = nettyServerConfig.getWorkerExecutor();
        int workerThreadCount = nettyServerConfig.getWorkerThreadCount();

        start(new NioServerSocketChannelFactory(bossExecutor, bossThreadCount, workerExecutor, workerThreadCount));
    }

    public void start(ServerChannelFactory serverChannelFactory)
    {
        if (!(InternalLoggerFactory.getDefaultFactory() instanceof Slf4JLoggerFactory)) {
            log.warn("Nifty always logs to slf4j, but netty is currently configured to use a " +
                     "different logging implementation. To correct this call " +
                     "InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory()) " +
                     "during your server's startup");
        }

        bootstrap = new ServerBootstrap(serverChannelFactory);
        bootstrap.setOptions(nettyServerConfig.getBootstrapOptions());
        bootstrap.setPipelineFactory(pipelineFactory);
        log.info("starting transport {}:{}", def.getName(), port);
        serverChannel = bootstrap.bind(new InetSocketAddress(port));
    }

    public void stop()
            throws InterruptedException
    {
        if (serverChannel != null) {
            log.info("stopping transport {}:{}", def.getName(), port);
            // first stop accepting
            final CountDownLatch latch = new CountDownLatch(1);
            serverChannel.close().addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception
                {
                    // stop and process remaining in-flight invocations
                    if (def.getExecutor() instanceof ExecutorService) {
                        ExecutorService exe = (ExecutorService) def.getExecutor();
                        ShutdownUtil.shutdownExecutor(exe, "dispatcher");
                    }
                    latch.countDown();
                }
            });
            latch.await();
            serverChannel = null;
        }
    }

    public Channel getServerChannel()
    {
        return serverChannel;
    }

    @Override
    public void releaseExternalResources()
    {
        bootstrap.releaseExternalResources();
    }

    private static class ConnectionLimiter extends SimpleChannelUpstreamHandler
    {
        private final AtomicInteger numConnections;
        private final int maxConnections;

        public ConnectionLimiter(int maxConnections)
        {
            this.maxConnections = maxConnections;
            this.numConnections = new AtomicInteger(0);
        }

        @Override
        @SuppressWarnings("PMD.CollapsibleIfStatements")
        public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
        {
            if (maxConnections > 0) {
                if (numConnections.incrementAndGet() > maxConnections) {
                    ctx.getChannel().close();
                    // numConnections will be decremented in channelClosed
                    log.info("Accepted connection above limit ({}). Dropping.", maxConnections);
                }
            }
            super.channelOpen(ctx, e);
        }

        @Override
        @SuppressWarnings("PMD.CollapsibleIfStatements")
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
        {
            if (maxConnections > 0) {
                if (numConnections.decrementAndGet() < 0) {
                    log.error("BUG in ConnectionLimiter");
                }
            }
            super.channelClosed(ctx, e);
        }
    }
}
