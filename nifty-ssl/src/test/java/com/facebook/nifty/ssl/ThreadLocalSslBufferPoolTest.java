/*
 * Copyright (C) 2012-2016 Facebook, Inc.
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
package com.facebook.nifty.ssl;

import org.jboss.netty.handler.ssl.SslBufferPool;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

public class ThreadLocalSslBufferPoolTest {
    @Test
    public void testMultipleThreads() {
        int numThreads = 5;
        int maxPoolSize = ThreadLocalSslBufferPool.getBufferSize() * numThreads;
        SslBufferPool pool = new ThreadLocalSslBufferPool(maxPoolSize, true, true, numThreads);
        Assert.assertEquals(pool.getMaxPoolSize(), ThreadLocalSslBufferPool.getBufferSize());
        Assert.assertEquals(pool.getUnacquiredPoolSize(), ThreadLocalSslBufferPool.getBufferSize());
    }

    @Test
    public void testPreallocated() {
        int maxPoolSize = ThreadLocalSslBufferPool.getBufferSize() * 5;
        SslBufferPool pool = new ThreadLocalSslBufferPool(maxPoolSize, true, true, 1);
        Assert.assertEquals(pool.getMaxPoolSize(), maxPoolSize);
        Assert.assertEquals(pool.getUnacquiredPoolSize(), maxPoolSize);

        ByteBuffer buf = null;
        try {
            buf = pool.acquireBuffer();
            Assert.assertEquals(buf.capacity(), ThreadLocalSslBufferPool.getBufferSize());
            Assert.assertEquals(pool.getUnacquiredPoolSize(), maxPoolSize - ThreadLocalSslBufferPool.getBufferSize());
        } finally {
            pool.releaseBuffer(buf);
        }
    }

    @Test
    public void testNonPreallocated() {
        int maxPoolSize = ThreadLocalSslBufferPool.getBufferSize() * 5;
        SslBufferPool pool = new ThreadLocalSslBufferPool(maxPoolSize, false, true, 1);
        Assert.assertEquals(pool.getMaxPoolSize(), maxPoolSize);
        Assert.assertEquals(pool.getUnacquiredPoolSize(), 0);

        ByteBuffer buf = null;
        try {
            buf = pool.acquireBuffer();
            Assert.assertEquals(buf.capacity(), ThreadLocalSslBufferPool.getBufferSize());
            Assert.assertEquals(pool.getUnacquiredPoolSize(), 0);
        } finally {
            pool.releaseBuffer(buf);
        }
        Assert.assertEquals(pool.getUnacquiredPoolSize(), ThreadLocalSslBufferPool.getBufferSize());
    }

    @Test
    public void testNonBlockingAcquireBuffer() {
        SslBufferPool pool = new ThreadLocalSslBufferPool(1, true, true, 1);

        ByteBuffer buf1 = null;
        ByteBuffer buf2 = null;
        try {
            buf1 = pool.acquireBuffer();
            Assert.assertEquals(pool.getUnacquiredPoolSize(), 0);
            // Try to acquire buffer. Unlike SslBufferPool, this should not block and will return another buffer.
            buf2 = pool.acquireBuffer();
        } finally {
            pool.releaseBuffer(buf1);
            pool.releaseBuffer(buf2);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidMaxPoolSize() {
        SslBufferPool pool = new ThreadLocalSslBufferPool(0, true, true, 1);
    }
}
