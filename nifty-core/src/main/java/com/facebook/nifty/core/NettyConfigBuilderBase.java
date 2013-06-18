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

import com.google.common.base.Preconditions;
import org.jboss.netty.util.Timer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/*
 * Hooks for configuring various parts of Netty.
 */
public abstract class NettyConfigBuilderBase<T extends NettyConfigBuilderBase<T>>
{
    // These constants come directly from Netty but are private in Netty.
    private static final int DEFAULT_BOSS_THREAD_COUNT = 1;
    private static final int DEFAULT_WORKER_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;

    private final Map<String, Object> options = new HashMap<>();
    private String niftyName;
    private int bossThreadCount = DEFAULT_BOSS_THREAD_COUNT;
    private int workerThreadCount = DEFAULT_WORKER_THREAD_COUNT;
    private ExecutorService bossThreadExecutor;
    private ExecutorService workerThreadExecutor;
    private Timer timer;

    public Map<String, Object> getBootstrapOptions()
    {
        return Collections.unmodifiableMap(options);
    }

    /**
     * Sets a netty {@link Timer} that will be used to trigger read, write, and connection
     * timeouts. Defaults to creating a new {@link org.jboss.netty.util.HashedWheelTimer}
     *
     * @param timer
     * @return
     */
    public T setTimer(Timer timer)
    {
        this.timer = timer;
        return (T) this;
    }

    protected Timer getTimer()
    {
        return timer;
    }

    /**
     * Sets an identifier which will be added to all thread created by the boss and worker
     * executors.
     *
     * @param niftyName
     * @return
     */
    protected T setNiftyName(String niftyName)
    {
        Preconditions.checkNotNull(niftyName, "niftyName cannot be null");
        this.niftyName = niftyName;
        return (T) this;
    }

    public String getNiftyName()
    {
        return niftyName;
    }

    public T setBossThreadExecutor(ExecutorService bossThreadExecutor)
    {
        this.bossThreadExecutor = bossThreadExecutor;
        return (T) this;
    }

    protected ExecutorService getBossExecutor()
    {
        return bossThreadExecutor;
    }

    /**
     * Sets the number of threads that will be used to manage
     * @param bossThreadCount
     * @return
     */
    public T setBossThreadCount(int bossThreadCount)
    {
        this.bossThreadCount = bossThreadCount;
        return (T) this;
    }

    protected int getBossThreadCount()
    {
        return bossThreadCount;
    }

    public T setWorkerThreadExecutor(ExecutorService workerThreadExecutor)
    {
        this.workerThreadExecutor = workerThreadExecutor;
        return (T) this;
    }

    protected ExecutorService getWorkerExecutor()
    {
        return workerThreadExecutor;
    }

    public T setWorkerThreadCount(int workerThreadCount)
    {
        this.workerThreadCount = workerThreadCount;
        return (T) this;
    }

    protected int getWorkerThreadCount()
    {
        return workerThreadCount;
    }

    // Magic alert ! Content of this class is considered ugly and magical.
    // For all intents and purposes this is to create a Map with the correct
    // key and value pairs for Netty's Bootstrap to consume.
    //
    // sadly Netty does not define any constant strings whatsoever for the proper key to
    // use and it's all based on standard java bean attributes.
    //
    // A ChannelConfig impl in netty is also tied with a socket, but since all
    // these configs are interfaces we can do a bit of magic hacking here.

    protected class Magic implements InvocationHandler
    {
        private final String prefix;

        public Magic(String prefix)
        {
            this.prefix = prefix;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable
        {
            // we are only interested in setters with single arg
            if (proxy != null) {
                if (method.getName().equals("toString")) {
                    return "this is a magic proxy";
                }
                else if (method.getName().equals("equals")) {
                    return Boolean.FALSE;
                }
                else if (method.getName().equals("hashCode")) {
                    return 0;
                }
            }
            // we don't support multi-arg setters
            if (method.getName().startsWith("set") && args.length == 1) {
                String attributeName = method.getName().substring(3);
                // camelCase it
                attributeName = attributeName.substring(0, 1).toLowerCase() + attributeName.substring(1);
                // now this is our key
                options.put(prefix + attributeName, args[0]);
                return null;
            }
            throw new UnsupportedOperationException();
        }
    }
}
