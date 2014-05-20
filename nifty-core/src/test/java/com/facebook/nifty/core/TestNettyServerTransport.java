package com.facebook.nifty.core;

import com.facebook.nifty.processor.NiftyProcessor;
import com.facebook.nifty.processor.NiftyProcessorFactory;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

public class TestNettyServerTransport {

    private NettyServerTransport newTransport(int minPort, int maxPort) {
        NiftyProcessorFactory processorFactory = new NiftyProcessorFactory()
        {
            @Override
            public NiftyProcessor getProcessor(TTransport transport)
            {
                return new NiftyProcessor() {
                    @Override
                    public ListenableFuture<Boolean> process(TProtocol in, TProtocol out, RequestContext requestContext) throws TException {
                        return null;
                    }
                };
            }
        };
        ThriftServerDef thriftServerDef = new ThriftServerDefBuilder().listen(minPort, maxPort)
                                                                      .withProcessorFactory(processorFactory).build();
        NettyServerConfig nettyServerConfig = NettyServerConfig.newBuilder().build();
        return new NettyServerTransport(thriftServerDef, nettyServerConfig, new DefaultChannelGroup());
    }

    private void tryStartTransport(int num, int minPort, int maxPort) throws InterruptedException {
        List<NettyServerTransport> transports = new LinkedList();
        try {
            for (int i = 0; i < num; i++) {
                NettyServerTransport transport = newTransport(minPort, maxPort);
                transport.start();
                transports.add(transport);
            }
        } finally {
            for (NettyServerTransport transport : transports) {
                transport.stop();
            }
        }
    }

    @Test(expectedExceptions = ChannelException.class, expectedExceptionsMessageRegExp = "Failed to bind to a range of ports.*")
    public void testPortRangeWithConflict() throws InterruptedException {
        tryStartTransport(11, 10000, 10009);
    }

    @Test
    public void testPortRangeWithoutConflict() throws InterruptedException {
        tryStartTransport(10, 10000, 10009);
    }
}
