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
package com.facebook.nifty.client;

import com.facebook.nifty.client.socks.Socks4ClientBootstrap;
import com.facebook.nifty.core.ShutdownUtil;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class NiftyClient implements Closeable
{
    // The constants come directly from Netty but are private in Netty. We need these default
    // values to call the NioClientSocketChannelFactory constructor with a custom timer.
    private static final int DEFAULT_BOSS_COUNT = 1;
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    public static final Duration DEFAULT_CONNECT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    public static final Duration DEFAULT_READ_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final Duration DEFAULT_WRITE_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final int DEFAULT_MAX_FRAME_SIZE = 16777216;

    private final NettyClientConfigBuilder configBuilder;
    private final ExecutorService bossExecutor;
    private final ExecutorService workerExecutor;
    private final NioClientSocketChannelFactory channelFactory;
    private final InetSocketAddress defaultSocksProxyAddress;
    private final ChannelGroup allChannels = new DefaultChannelGroup();
    private final HashedWheelTimer hashedWheelTimer;

    /**
     * Creates a new NiftyClient with defaults: cachedThreadPool for bossExecutor and workerExecutor
     */
    public NiftyClient()
    {
        this(new NettyClientConfigBuilder());
    }

    public NiftyClient(NettyClientConfigBuilder configBuilder)
    {
        this(configBuilder, null);
    }

    public NiftyClient(
            NettyClientConfigBuilder configBuilder,
            @Nullable InetSocketAddress defaultSocksProxyAddress)
    {
        this.configBuilder = configBuilder;

        this.hashedWheelTimer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("nifty-client-timer-%s").setDaemon(true).build());
        this.bossExecutor = newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("nifty-client-boss-%s").setDaemon(true).build());
        this.workerExecutor = newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("nifty-client-worker-%s").setDaemon(true).build());
        this.defaultSocksProxyAddress = defaultSocksProxyAddress;
        NioWorkerPool workerPool = new NioWorkerPool(workerExecutor, DEFAULT_IO_THREADS);
        this.channelFactory = new NioClientSocketChannelFactory(bossExecutor, DEFAULT_BOSS_COUNT, workerPool, hashedWheelTimer);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector)
    {
        return connectAsync(clientChannelConnector,
                            DEFAULT_CONNECT_TIMEOUT,
                            DEFAULT_READ_TIMEOUT,
                            DEFAULT_WRITE_TIMEOUT,
                            DEFAULT_MAX_FRAME_SIZE,
                            defaultSocksProxyAddress);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize)
    {
        return connectAsync(clientChannelConnector,
                            connectTimeout,
                            receiveTimeout,
                            sendTimeout,
                            maxFrameSize,
                            defaultSocksProxyAddress);
    }

    public <T extends NiftyClientChannel> ListenableFuture<T> connectAsync(
            NiftyClientConnector<T> clientChannelConnector,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize,
            @Nullable InetSocketAddress socksProxyAddress)
    {
        ClientBootstrap bootstrap = createClientBootstrap(socksProxyAddress);
        bootstrap.setOptions(configBuilder.getOptions());
        bootstrap.setOption("connectTimeoutMillis", (long)connectTimeout.toMillis());
        bootstrap.setPipelineFactory(clientChannelConnector.newChannelPipelineFactory(maxFrameSize));
        ChannelFuture nettyChannelFuture = clientChannelConnector.connect(bootstrap);
        nettyChannelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel channel = future.getChannel();
                if (channel != null && channel.isOpen()) {
                    // Add the channel to allChannels, and set it up to be removed when closed
                    allChannels.add(channel);
                    channel.getCloseFuture().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            Channel channel = future.getChannel();
                            allChannels.remove(channel);
                        }
                    });
                }
            }
        });
        return new TNiftyFuture<>(clientChannelConnector,
                                  receiveTimeout,
                                  sendTimeout,
                                  nettyChannelFuture);
    }

    // trying to mirror the synchronous nature of TSocket as much as possible here.
    public TNiftyClientTransport connectSync(InetSocketAddress addr)
            throws TTransportException, InterruptedException
    {
        return connectSync(addr, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_WRITE_TIMEOUT, DEFAULT_MAX_FRAME_SIZE);
    }

    public TNiftyClientTransport connectSync(
            InetSocketAddress addr,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize)
            throws TTransportException, InterruptedException
    {
        return connectSync(addr, connectTimeout, receiveTimeout, sendTimeout, maxFrameSize, defaultSocksProxyAddress);
    }

    public TNiftyClientTransport connectSync(
            InetSocketAddress addr,
            Duration connectTimeout,
            Duration receiveTimeout,
            Duration sendTimeout,
            int maxFrameSize,
            @Nullable InetSocketAddress socksProxyAddress)
            throws TTransportException, InterruptedException
    {
        // TODO: implement send timeout for sync client
        ClientBootstrap bootstrap = createClientBootstrap(socksProxyAddress);
        bootstrap.setOptions(configBuilder.getOptions());
        bootstrap.setOption("connectTimeoutMillis", (long) connectTimeout.toMillis());
        bootstrap.setPipelineFactory(new NiftyClientChannelPipelineFactory(maxFrameSize));
        ChannelFuture f = bootstrap.connect(addr);
        f.await();
        Channel channel = f.getChannel();
        if (f.getCause() != null) {
            String message = String.format("unable to connect to %s:%d %s",
                    addr.getHostName(),
                    addr.getPort(),
                    socksProxyAddress == null ? "" : "via socks proxy at " + socksProxyAddress);
            throw new TTransportException(message, f.getCause());
        }

        if (f.isSuccess() && (channel != null)) {
            if (channel.isOpen()) {
                // Add the channel to allChannels, and set it up to be removed when closed
                allChannels.add(channel);
                channel.getCloseFuture().addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        Channel channel = future.getChannel();
                        allChannels.remove(channel);
                    }
                });
            }

            TNiftyClientTransport transport = new TNiftyClientTransport(channel, receiveTimeout);
            channel.getPipeline().addLast("thrift", transport);
            return transport;
        }

        throw new TTransportException(String.format(
                "unknown error connecting to %s:%d %s",
                addr.getHostName(),
                addr.getPort(),
                socksProxyAddress == null ? "" : "via socks proxy at " + socksProxyAddress
        ));
    }

    @Override
    public void close()
    {
        // Stop the timer thread first, so no timeouts can fire during the rest of the
        // shutdown process
        hashedWheelTimer.stop();

        ShutdownUtil.shutdownChannelFactory(channelFactory,
                                            bossExecutor,
                                            workerExecutor,
                                            allChannels);
    }

    private ClientBootstrap createClientBootstrap(InetSocketAddress socksProxyAddress)
    {
        if (socksProxyAddress != null) {
            return new Socks4ClientBootstrap(channelFactory, socksProxyAddress);
        }
        else {
            return new ClientBootstrap(channelFactory);
        }
    }

    private class TNiftyFuture<T extends NiftyClientChannel> extends AbstractFuture<T>
    {
        private TNiftyFuture(final NiftyClientConnector<T> clientChannelConnector,
                             final Duration receiveTimeout,
                             final Duration sendTimeout,
                             final ChannelFuture channelFuture)
        {
            channelFuture.addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception
                {
                    if (future.isSuccess()) {
                        Channel nettyChannel = future.getChannel();
                        T channel = clientChannelConnector.newThriftClientChannel(nettyChannel,
                                                                                  hashedWheelTimer);
                        channel.setReceiveTimeout(receiveTimeout);
                        channel.setSendTimeout(sendTimeout);
                        set(channel);
                    }
                    else if (future.isCancelled()) {
                        cancel(true);
                    }
                    else {
                        setException(future.getCause());
                    }
                }
            });
        }
    }
}
