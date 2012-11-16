/*
 * Copyright (C) 2012 Facebook, Inc.
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
package com.facebook.nifty.server;

import com.facebook.nifty.client.FramedClientChannel;
import com.facebook.nifty.client.NiftyClient;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.fail;

public class TestNiftyClientTimeout
{
    private static final Duration TEST_CONNECT_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_READ_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);
    private static final Duration TEST_WRITE_TIMEOUT = new Duration(500, TimeUnit.MILLISECONDS);

    @BeforeTest(alwaysRun = true)
    public void setup() throws IOException
    {
        System.setProperty("java.nio.channels.spi.SelectorProvider", DeafSelectorProvider.class.getName());
    }

    @AfterTest(alwaysRun = true)
    public void tearDown()
    {
        System.clearProperty("java.nio.channels.spi.SelectorProvider");
    }

    @Test(timeOut = 2000)
    public void testSyncConnectTimeout() throws ConnectException, IOException
    {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();

        final NiftyClient client = new NiftyClient();
        try {
                client.connectSync(new InetSocketAddress(port),
                                   TEST_CONNECT_TIMEOUT,
                                   TEST_READ_TIMEOUT,
                                   TEST_WRITE_TIMEOUT);
        }
        catch (Throwable throwable) {
            if (isTimeoutException(throwable)) {
                return;
            }
            Throwables.propagate(throwable);
        }
        finally {
            client.close();
            serverSocket.close();
        }

        // Should never get here
        fail("Connection succeeded but failure was expected");
    }

    @Test(timeOut = 2000)
    public void testAsyncConnectTimeout() throws IOException
    {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();

        final NiftyClient client = new NiftyClient();
        try {
            ListenableFuture<FramedClientChannel> future =
                            client.connectAsync(new FramedClientChannel.Factory(),
                                                new InetSocketAddress(port),
                                                TEST_CONNECT_TIMEOUT,
                                                TEST_READ_TIMEOUT,
                                                TEST_WRITE_TIMEOUT);
            // Wait while NiftyClient attempts to connect the channel
            future.get();
        }
        catch (Throwable throwable) {
            if (isTimeoutException(throwable)) {
                return;
            }
            Throwables.propagate(throwable);
        }
        finally {
            client.close();
            serverSocket.close();
        }

        // Should never get here
        fail("Connection succeeded but failure was expected");
    }

    private boolean isTimeoutException(Throwable throwable) {
        Throwable rootCause = Throwables.getRootCause(throwable);
        // Look for a java.net.ConnectException, with the message "connection timed out"
        return (rootCause instanceof ConnectException &&
                rootCause.getMessage().compareTo("connection timed out") == 0);
    }
}
