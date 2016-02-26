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
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.facebook.nifty.processor.NiftyProcessor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

public class TestConnectionContext extends AbstractLiveTest
{
    @Test
    public void testContextNormal() throws IOException, TException, InterruptedException
    {
        final SynchronousQueue<RequestContext> requestContextQueue = new SynchronousQueue<>();
        final SynchronousQueue<SettableFuture<ListenableFuture<Boolean>>> sendResponseQueue = new SynchronousQueue<>();
        NiftyProcessor processor = mockProcessor(null, null, requestContextQueue, sendResponseQueue);

        try (FakeServer server = listen(processor);
             FakeClient client = connect(server)) {

            // Issue a fake request and wait for it to arrive
            client.sendRequest();
            RequestContext requestContext = requestContextQueue.poll(30, TimeUnit.SECONDS);
            Preconditions.checkNotNull(requestContext, "Either deadlock, or your computer is really slow");
            ConnectionContext actualContext = requestContext.getConnectionContext();
            SettableFuture<ListenableFuture<Boolean>> sendResponseFuture = sendResponseQueue.take();

            // At this point, the server has called the mock processor's process method.
            // Values in the ConnectionContext should be right
            Assert.assertNotNull(
                    actualContext.getRemoteAddress(),
                    "remote address non-null");
            Assert.assertEquals(
                    ((InetSocketAddress) actualContext.getRemoteAddress()).getPort(),
                    client.getClientPort(),
                    "context has correct port");
            Assert.assertFalse(
                    actualContext.hasClientDisconnected(),
                    "should not report that client has disconnected");

            // Allow the process method to return a future, but don't fulfill it yet.
            SettableFuture<Boolean> resp = SettableFuture.create();
            sendResponseFuture.set(resp);

            // The context should still be right
            Assert.assertNotNull(
                actualContext.getRemoteAddress(),
                "remote address non-null");
            Assert.assertEquals(
                    ((InetSocketAddress) actualContext.getRemoteAddress()).getPort(),
                    client.getClientPort(),
                    "context has correct port");
            Assert.assertFalse(
                    actualContext.hasClientDisconnected(),
                    "should not report that client has disconnected");

            // Finish
            resp.set(false);
        }
    }

    @Test
    public void testDisconnectBetweenQueueAndProcessing() throws IOException, TException, InterruptedException
    {
        // An ExecutorService which lets us delay calls from NiftyDispatcher to the processor
        final SynchronousQueue<Semaphore> tasksWaitingToRun = new SynchronousQueue<>();
        final ExecutorService threadpool = Executors.newCachedThreadPool();
        Executor slowExecutor = new Executor() {
            @Override
            public void execute(final Runnable task) {
                threadpool.execute(new Runnable() {
                    @Override
                    public void run() {
                        Semaphore allowMeToContinue = new Semaphore(0);
                        Uninterruptibles.putUninterruptibly(tasksWaitingToRun, allowMeToContinue);
                        allowMeToContinue.acquireUninterruptibly();
                        task.run();
                    }
                });
            }
        };

        final SynchronousQueue<RequestContext> requestContextQueue = new SynchronousQueue<>();
        final SynchronousQueue<SettableFuture<ListenableFuture<Boolean>>> sendResponseQueue = new SynchronousQueue<>();
        NiftyProcessor processor = mockProcessor(null, null, requestContextQueue, sendResponseQueue);

        ChannelGroup channels = new DefaultChannelGroup();

        try (FakeServer server = listen(processor, slowExecutor, channels);
             FakeClient client = connect(server)) {

            // Send a request, and wait for NiftyDispatcher to put it in the queue
            client.sendRequest();
            Semaphore allowNiftyProcessorToRun = tasksWaitingToRun.poll(30, TimeUnit.SECONDS);
            Preconditions.checkNotNull(allowNiftyProcessorToRun, "Either deadlock, or your computer is really slow");

            // Close the channel. We do the close on the server side because that
            // lets us control when all the close handlers are run.
            closeAndWaitForHandlers(Iterables.getOnlyElement(channels));

            // Allow nifty to call into the processor, but don't let the processor return
            allowNiftyProcessorToRun.release();
            RequestContext requestContext = requestContextQueue.poll(30, TimeUnit.SECONDS);
            Preconditions.checkNotNull(requestContext, "Either deadlock, or your computer is really slow");
            ConnectionContext actualContext = requestContext.getConnectionContext();
            SettableFuture<ListenableFuture<Boolean>> sendResponse = sendResponseQueue.take();

            // The connection context should be correct
            Assert.assertNotNull(
                    actualContext.getRemoteAddress(),
                    "remote address non-null");
            Assert.assertEquals(
                    ((InetSocketAddress) actualContext.getRemoteAddress()).getPort(),
                    client.getClientPort(),
                    "context has correct port");
            Assert.assertTrue(
                    actualContext.hasClientDisconnected(),
                    "context shows remote client disconnected");

            // Finish up
            sendResponse.set(Futures.immediateFuture(false));
        } finally {
            threadpool.shutdown();
        }
    }

    @Test
    public void testDisconnectWhenProcessing() throws IOException, TException, InterruptedException
    {
        final SynchronousQueue<RequestContext> requestContextQueue = new SynchronousQueue<>();
        final SynchronousQueue<SettableFuture<ListenableFuture<Boolean>>> sendResponseQueue = new SynchronousQueue<>();
        NiftyProcessor processor = mockProcessor(null, null, requestContextQueue, sendResponseQueue);

        ChannelGroup channels = new DefaultChannelGroup();

        try (FakeServer server = listen(processor, Executors.newCachedThreadPool(), channels);
             FakeClient client = connect(server)) {

            // Issue a fake request and wait for it to arrive
            client.sendRequest();
            RequestContext requestContext = requestContextQueue.poll(30, TimeUnit.SECONDS);
            Preconditions.checkNotNull(requestContext, "Either deadlock, or your computer is really slow");
            ConnectionContext actualContext = requestContext.getConnectionContext();
            SettableFuture<ListenableFuture<Boolean>> sendResponseFuture = sendResponseQueue.take();

            // At this point, the server has called the mock processor's process method.
            // Values in the ConnectionContext should be right
            Assert.assertNotNull(
                    actualContext.getRemoteAddress(),
                    "remote address non-null");
            Assert.assertEquals(
                    ((InetSocketAddress) actualContext.getRemoteAddress()).getPort(),
                    client.getClientPort(),
                    "context has correct port");
            Assert.assertFalse(
                    actualContext.hasClientDisconnected(),
                    "should not report that client has disconnected");

            // Close the channel. We do the close on the server side because that
            // lets us control when all the close handlers are run.
            closeAndWaitForHandlers(Iterables.getOnlyElement(channels));

            // The connection context should be correct, and pick up on the disconnect
            Assert.assertNotNull(
                    actualContext.getRemoteAddress(),
                    "remote address non-null");
            Assert.assertEquals(
                    ((InetSocketAddress) actualContext.getRemoteAddress()).getPort(),
                    client.getClientPort(),
                    "context has correct port");
            Assert.assertTrue(
                    actualContext.hasClientDisconnected(),
                    "should report that client has disconnected");

            // Tidy up
            sendResponseFuture.set(Futures.immediateFuture(false));
        }
    }
}
