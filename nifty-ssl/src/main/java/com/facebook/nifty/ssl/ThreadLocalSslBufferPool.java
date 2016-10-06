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

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.facebook.nifty.core.NettyConfigBuilderBase.DEFAULT_WORKER_THREAD_COUNT;

public class ThreadLocalSslBufferPool extends SslBufferPool {
    private final ThreadLocal<NonBlockingSslBufferPool> pool;

    public ThreadLocalSslBufferPool(int maxPoolSize, boolean preallocate, boolean allocateDirect) {
        this(maxPoolSize, preallocate, allocateDirect, DEFAULT_WORKER_THREAD_COUNT);
    }

    public ThreadLocalSslBufferPool(int maxPoolSize, boolean preallocate, boolean allocateDirect, int numWorkerThreads) {
        if (maxPoolSize <= 0) {
            throw new IllegalArgumentException("maxPoolSize: " + maxPoolSize);
        }
        pool = new ThreadLocal<NonBlockingSslBufferPool>() {
            @Override
            protected NonBlockingSslBufferPool initialValue() {
                int threadLocalMaxPoolSize = maxPoolSize / numWorkerThreads;
                if (maxPoolSize % numWorkerThreads != 0) {
                    threadLocalMaxPoolSize++;
                }
                return new NonBlockingSslBufferPool(threadLocalMaxPoolSize, preallocate, allocateDirect);
            }
        };
    }

    @Override
    public int getMaxPoolSize() {
        return pool.get().getMaxPoolSize();
    }

    @Override
    public int getUnacquiredPoolSize() {
        return pool.get().getUnacquiredPoolSize();
    }

    @Override
    public ByteBuffer acquireBuffer() {
        return pool.get().acquireBuffer();
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
        pool.get().releaseBuffer(buffer);
    }

    public static int getBufferSize() { return NonBlockingSslBufferPool.getBufferSize(); }

    /**
     * A lot of this code is taken from Netty's SslBufferPool.
     * https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/ssl/SslBufferPool.java
     * If we use threadLocal SslBufferPool with a maxPoolSize limit and the thread runs out of preallocated buffer
     * space, the IO thread will block as it waits for memory to freed and will deadlock. NonBlockingSslBufferPoll
     * allows us to create a SslBufferPool that will not block when the thread runs out of preallocated buffer space.
     */
    private static class NonBlockingSslBufferPool {
        private static final int MAX_PLAINTEXT_LENGTH = 16 * 1024; // 2^14
        private static final int MAX_COMPRESSED_LENGTH = MAX_PLAINTEXT_LENGTH + 1024;
        private static final int MAX_CIPHERTEXT_LENGTH = MAX_COMPRESSED_LENGTH + 1024;

        // Header (5) + Data (2^14) + Compression (1024) + Encryption (1024) + MAC (20) + Padding (256)
        private static final int MAX_ENCRYPTED_PACKET_LENGTH = MAX_CIPHERTEXT_LENGTH + 5 + 20 + 256;

        // Add 1024 as a room for compressed data and another 1024 for Apache Harmony compatibility.
        private static final int MAX_PACKET_SIZE_ALIGNED = (MAX_ENCRYPTED_PACKET_LENGTH / 128 + 1) * 128;

        private final BlockingQueue<ByteBuffer> pool;
        private final int maxBufferCount;
        private final boolean allocateDirect;

        public NonBlockingSslBufferPool(int maxPoolSize, boolean preallocate, boolean allocateDirect) {
            int maxBufferCount = maxPoolSize / MAX_PACKET_SIZE_ALIGNED;
            if (maxPoolSize % MAX_PACKET_SIZE_ALIGNED != 0) {
                maxBufferCount++;
            }

            this.maxBufferCount = maxBufferCount;
            this.allocateDirect = allocateDirect;

            pool = new ArrayBlockingQueue<ByteBuffer>(maxBufferCount);

            if (preallocate) {
                ByteBuffer preallocated = allocate(maxBufferCount * MAX_PACKET_SIZE_ALIGNED);
                for (int i = 0; i < maxBufferCount; i++) {
                    int pos = i * MAX_PACKET_SIZE_ALIGNED;
                    preallocated.clear().position(pos).limit(pos + MAX_PACKET_SIZE_ALIGNED);
                    pool.add(preallocated.slice());
                }
            }
        }

        public int getMaxPoolSize() {
            return maxBufferCount * MAX_PACKET_SIZE_ALIGNED;
        }

        public int getUnacquiredPoolSize() {
            return pool.size() * MAX_PACKET_SIZE_ALIGNED;
        }

        public static int getBufferSize() { return MAX_PACKET_SIZE_ALIGNED; }

        public ByteBuffer acquireBuffer() {
            ByteBuffer buf = pool.poll();
            if (buf == null) {
                buf = allocate(MAX_PACKET_SIZE_ALIGNED);
            }

            buf.clear();
            return buf;
        }

        public void releaseBuffer(ByteBuffer buffer) {
            if (buffer != null) {
                pool.offer(buffer);
            }
        }

        private ByteBuffer allocate(int capacity) {
            if (allocateDirect) {
                return ByteBuffer.allocateDirect(capacity);
            } else {
                return ByteBuffer.allocate(capacity);
            }
        }
    }
}
