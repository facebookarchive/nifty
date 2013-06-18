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
package com.facebook.nifty.guice;

import com.facebook.nifty.core.NettyServerConfig;
import com.facebook.nifty.core.NettyServerConfigBuilder;
import com.facebook.nifty.core.ThriftServerDef;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.util.Providers;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import javax.inject.Provider;
import javax.inject.Singleton;

public abstract class NiftyModule extends AbstractModule
{
    private boolean configBound = false;

    @Override
    protected void configure()
    {
        configureNifty();
    }

    @Provides
    @Singleton
    public ChannelGroup getChannelGroup()
    {
        return new DefaultChannelGroup();
    }

    public NiftyModule useDefaultNettyServerConfig()
    {
        withNettyServerConfig(new Provider<NettyServerConfig>() {
            @Override
            public NettyServerConfig get()
            {
                return NettyServerConfig.newBuilder().build();
            }
        });
        return this;
    }

    public NiftyModule withNettyServerConfig(Class<? extends Provider<NettyServerConfig>> providerClass)
    {
        if (!configBound) {
            binder().bind(NettyServerConfig.class).toProvider(providerClass);
            configBound = true;
            return this;
        }
        throw iae();
    }

    public NiftyModule withNettyServerConfig(Provider<NettyServerConfig> provider)
    {
        if (!configBound) {
            // workaround for guice issue # 487
            com.google.inject.Provider<NettyServerConfig> guiceProvider = Providers.guicify(provider);
            binder().bind(NettyServerConfig.class).toProvider(guiceProvider);
            configBound = true;
            return this;
        }
        throw iae();
    }

    /**
     * User of Nifty via guice should override this method and use the little DSL defined here.
     */
    protected abstract void configureNifty();

    protected NiftyBuilder bind()
    {
        return new NiftyBuilder();
    }

    protected class NiftyBuilder
    {
        public NiftyBuilder()
        {
        }

        public void toInstance(ThriftServerDef def)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().toInstance(def);
        }

        public void toProvider(Class<? extends Provider<ThriftServerDef>> provider)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().toProvider(provider).asEagerSingleton();
        }

        public void toProvider(Provider<? extends ThriftServerDef> provider)
        {
            // workaround for guice issue # 487
            com.google.inject.Provider<? extends ThriftServerDef> guiceProvider = Providers.guicify(provider);
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().toProvider(guiceProvider).asEagerSingleton();
        }

        public void toProvider(TypeLiteral<? extends javax.inject.Provider<? extends ThriftServerDef>> typeLiteral)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().toProvider(typeLiteral).asEagerSingleton();
        }

        public void toProvider(com.google.inject.Key<? extends javax.inject.Provider<? extends ThriftServerDef>> key)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().toProvider(key).asEagerSingleton();
        }

        public void to(Class<? extends ThriftServerDef> clazz)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().to(clazz).asEagerSingleton();
        }

        public void to(TypeLiteral<? extends ThriftServerDef> typeLiteral)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().to(typeLiteral).asEagerSingleton();
        }

        public void to(com.google.inject.Key<? extends ThriftServerDef> key)
        {
            Multibinder.newSetBinder(binder(), ThriftServerDef.class)
                    .addBinding().to(key).asEagerSingleton();
        }
    }

    private IllegalStateException iae()
    {
        return new IllegalStateException("Config already bound! Call useDefaultNettyServerConfig() or withNettyServerConfig() only once");
    }
}
