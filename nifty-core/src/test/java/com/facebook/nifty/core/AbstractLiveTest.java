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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TIOStreamTransport;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.facebook.nifty.processor.NiftyProcessor;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

public class AbstractLiveTest
{
    protected AbstractLiveTest() { }


    protected FakeServer listen(NiftyProcessor processor) {
        // NiftyBootstrap.stop() will shutdown the threadpool for us
        return new FakeServer(processor, Executors.newCachedThreadPool(), new DefaultChannelGroup());
    }

    protected FakeServer listen(NiftyProcessor processor, Executor taskExecutor, ChannelGroup group) {
        return new FakeServer(processor, taskExecutor, group);
    }

    protected FakeClient connect(FakeServer server) throws IOException {
        return new FakeClient(server);
    }

    /**
     * Makes a NiftyProcessor which can be controlled from another thread.
     * The intent is that a test case can make a mock NiftyProcessor using this
     * method and pass it to a FakeServer. The test case can then use the features
     * of the mock processor to control precisely when various steps of the
     * server-side processing happens, and make assertions when it needs to.
     *
     * Whenever a call reaches the mock processor, the processor first passes
     * the arguments to the test case by adding them to the end of queues. Then,
     * it passes the test case a {@code SettableFuture} to control how it will
     * return to nifty. The test case is responsible for inspecting the arguments
     * as it wants, and eventually fulfilling the {@code SettableFuture} thereby
     * causing the processor to return to nifty. (Note: the mock processor creates
     * the SettableFuture and waits on it; the test case uses the "settable" part.
     * This is different from most applications of SettableFuture, where the
     * SettableFuture's creator plans to set it and the other party waits.)
     * If the test case fulfills the SettableFuture with a value, the mock processor
     * will return that value to nifty; since the value is itself a Future, the
     * test case is responsible for fulfilling it too. If the test case fulfills
     * the SettableFuture with an exception, the mock processor will throw that
     * exception verbatim.
     *
     * @param inQueue
     *          an optional queue, in which the mock processor will put the
     *          input TProtocol arguments of any calls it gets
     * @param outQueue
     *          an optional queue, in which the mock processor will put the
     *          output TProtocol argument of any calls it gets
     * @param requestContextQueue
     *          an optional queue, in which the mock processor will put the
     *          RequestContext argument of any calls it gets
     * @param pendingResponsesQueue
     *          a queue in which the mock processor will place a SettableFuture
     *          for every call it gets, which the mock processor's creator is
     *          responsible for fulfilling
     * @return the new mock processor
     */
    protected NiftyProcessor mockProcessor(
            @Nullable final BlockingQueue<TProtocol> inQueue,
            @Nullable final BlockingQueue<TProtocol> outQueue,
            @Nullable final BlockingQueue<RequestContext> requestContextQueue,
            @Nonnull final BlockingQueue<SettableFuture<ListenableFuture<Boolean>>> pendingResponsesQueue
    ) {
        return new NiftyProcessor() {
            @Override
            public ListenableFuture<Boolean> process(TProtocol in, TProtocol out,
                            RequestContext requestContext) throws TException {
                if (inQueue != null) {
                    Uninterruptibles.putUninterruptibly(inQueue, in);
                }
                if (outQueue != null) {
                    Uninterruptibles.putUninterruptibly(outQueue, out);
                }
                if (requestContextQueue != null) {
                    Uninterruptibles.putUninterruptibly(requestContextQueue, requestContext);
                }
                SettableFuture<ListenableFuture<Boolean>> respFutureFuture = SettableFuture.create();
                Uninterruptibles.putUninterruptibly(pendingResponsesQueue, respFutureFuture);
                try {
                  return Uninterruptibles.getUninterruptibly(respFutureFuture);
                } catch (CancellationException e) {
                  throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                  // Sneaky throw exactly what was in e.
                  class SneakyThrow<T extends Throwable> {
                    @SuppressWarnings("unchecked")
                    public void thro(Object t) throws T { throw (T) t; }
                  }
                  new SneakyThrow<RuntimeException>().thro(e.getCause());
                  throw new AssertionError("not reached");
                }
            }
        };
    }


    protected static class FakeServer implements AutoCloseable {
        private final NiftyBootstrap nifty;

        private FakeServer(NiftyProcessor processor, Executor taskExecutor, ChannelGroup group) {
            ThriftServerDef thriftServerDef =
                new ThriftServerDefBuilder()
                .withProcessor(processor)
                .using(taskExecutor)
                .listen(0)
                .build();

            this.nifty = new NiftyBootstrap(
                            ImmutableSet.of(thriftServerDef),
                            new NettyServerConfigBuilder().build(),
                            group);

            nifty.start();
        }

        public int getPort() {
            return Iterables.getOnlyElement(nifty.getBoundPorts().values());
        }

        @Override
        public void close() {
            nifty.stop();
        }
    }

    protected static class FakeClient implements AutoCloseable {
        private Socket socketToServer;

        private FakeClient(FakeServer server) throws IOException {
            socketToServer = new Socket(InetAddress.getLoopbackAddress(), server.getPort());
        }

        public int getClientPort() {
            return socketToServer.getLocalPort();
        }

        public void sendRequest() throws IOException, TException {
            TProtocol out = new TBinaryProtocol(new TIOStreamTransport(socketToServer.getOutputStream()));
            out.writeMessageBegin(new TMessage("dummy", TMessageType.CALL, 0));
            out.writeStructBegin(new TStruct("dummy_args"));
            out.writeFieldStop();
            out.writeStructEnd();
            out.writeMessageEnd();
            out.getTransport().flush();
        }

        @Override
        public void close() throws IOException {
            socketToServer.close();
            socketToServer = null;
        }
    }

    protected static void closeAndWaitForHandlers(Channel channel) {
        // Unfortunately, netty doesn't give us a way to actually wait for handlers.
        // The best way can do is to trigger the close on the IO thread, and confirm
        // that it runs them on the same thread.

        AtomicReference<Thread> disconnectedThread = new AtomicReference<Thread>();
        AtomicReference<Thread> closedThread = new AtomicReference<Thread>();
        AtomicReference<Thread> ioThread = new AtomicReference<Thread>();

        channel.getPipeline().addFirst(
            "tests are hard",
            new SimpleChannelUpstreamHandler() {
                @Override
                public void channelDisconnected(ChannelHandlerContext ctx,
                        ChannelStateEvent e) throws Exception {
                    super.channelDisconnected(ctx, e);
                    disconnectedThread.set(Thread.currentThread());
                }

                @Override
                public void channelClosed(ChannelHandlerContext ctx,
                        ChannelStateEvent e) throws Exception {
                    super.channelClosed(ctx, e);
                    closedThread.set(Thread.currentThread());
                }
            }
        );

        channel.getPipeline().execute(() -> {
            channel.close();
            ioThread.set(Thread.currentThread());
        }).syncUninterruptibly();

        Preconditions.checkState(ioThread.get() == closedThread.get());
        Preconditions.checkState(ioThread.get() == disconnectedThread.get());
    }
}
