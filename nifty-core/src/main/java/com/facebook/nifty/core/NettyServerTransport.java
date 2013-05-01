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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

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
    private final ThriftServerDef thriftServerDef;
    private final NettyConfigBuilder configBuilder;
    private final ChannelGroup allChannels;
    private final Timer timer;
    private final ServerStopFuture serverStopFuture = new ServerStopFuture();

    @Inject
    public NettyServerTransport(
            final ThriftServerDef thriftServerDef,
            NettyConfigBuilder configBuilder,
            final ChannelGroup allChannels,
            final Timer timer)
    {
        this.thriftServerDef = thriftServerDef;
        this.configBuilder = configBuilder;
        this.allChannels = allChannels;
        this.timer = timer;
        this.port = thriftServerDef.getServerPort();
        if (thriftServerDef.isHeaderTransport()) {
            this.pipelineFactory = getHeaderChannelPipelineFactory();
        }
        else {
            this.pipelineFactory = getStandardChannelPipelineFactory();
        }
    }

    public synchronized void start()
    {
        start(new NioServerSocketChannelFactory());
    }

    public synchronized void start(ExecutorService bossExecutor, ExecutorService workerExecutor)
    {
        start(new NioServerSocketChannelFactory(bossExecutor, workerExecutor));
    }

    public synchronized void start(ServerChannelFactory serverChannelFactory)
    {
        // Server is already running
        checkState(!isRunning());

        // Server has already been started and stopped
        checkState(!serverStopFuture.isDone());

        bootstrap = new ServerBootstrap(serverChannelFactory);
        bootstrap.setOptions(configBuilder.getOptions());
        bootstrap.setPipelineFactory(pipelineFactory);
        log.info("starting transport {}:{}", thriftServerDef.getName(), port);
        serverChannel = bootstrap.bind(new InetSocketAddress(port));
    }

    public synchronized void stop()
            throws InterruptedException
    {
        if (serverChannel != null) {
            log.info("stopping transport {}:{}", thriftServerDef.getName(), port);
            // first stop accepting
            final CountDownLatch latch = new CountDownLatch(1);
            serverChannel.close().addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception
                {
                    // stop and process remaining in-flight invocations
                    if (thriftServerDef.getExecutor() instanceof ExecutorService) {
                        ExecutorService exe = (ExecutorService) thriftServerDef.getExecutor();
                        ShutdownUtil.shutdownExecutor(exe, "dispatcher");
                    }
                    latch.countDown();
                }
            });
            latch.await();
            serverChannel = null;

            serverStopFuture.setServerStopped();
        }
    }

    public boolean isRunning()
    {
        return getServerChannel() != null;
    }

    public Channel getServerChannel()
    {
        return serverChannel;
    }

    @Override
    public synchronized void releaseExternalResources()
    {
        bootstrap.releaseExternalResources();
    }

    protected synchronized ChannelPipelineFactory getStandardChannelPipelineFactory()
    {
        return new ChannelPipelineFactory()
        {
            @Override
            public ChannelPipeline getPipeline()
                    throws Exception
            {
                ChannelPipeline cp = Channels.pipeline();
                cp.addLast(ChannelStatistics.NAME, new ChannelStatistics(allChannels));
                cp.addLast("frameDecoder", new ThriftMessageDecoder(thriftServerDef.getMaxFrameSize(),
                                                                    thriftServerDef.getInProtocolFactory()));
                if (thriftServerDef.getClientIdleTimeout() != null) {
                    // Add handlers to detect idle client connections and disconnect them
                    cp.addLast("idleTimeoutHandler", new IdleStateHandler(timer,
                                                                          (int) thriftServerDef.getClientIdleTimeout().toMillis(),
                                                                          NO_WRITER_IDLE_TIMEOUT,
                                                                          NO_ALL_IDLE_TIMEOUT,
                                                                          TimeUnit.MILLISECONDS
                    ));
                    cp.addLast("idleDisconnectHandler", new IdleDisconnectHandler());
                }
                cp.addLast("dispatcher", new NiftyDispatcher(thriftServerDef));
                return cp;
            }
        };
    }

    protected synchronized ChannelPipelineFactory getHeaderChannelPipelineFactory()
    {
        throw new UnsupportedOperationException("ASF version does not support THeaderTransport !");
    }

    /**
     * Returns a {@link ListenableFuture} that will complete when the server has been stopped
     * @return
     */
    public synchronized ListenableFuture<Void> getStopFuture()
    {
        return serverStopFuture;
    }

    protected final ChannelGroup getAllChannels()
    {
        return allChannels;
    }

    protected final ThriftServerDef getThriftServerDef()
    {
        return thriftServerDef;
    }

    private static class ServerStopFuture extends AbstractFuture<Void>
    {
        public void setServerStopped()
        {
            set(null);
        }
    }
}
