package com.facebook.nifty.server;

import com.facebook.nifty.client.NiftyClient;
import com.facebook.nifty.core.*;
import com.facebook.nifty.guice.NiftyModule;
import com.facebook.nifty.test.LogEntry;
import com.facebook.nifty.test.ResultCode;
import com.facebook.nifty.test.scribe;
import com.google.inject.Guice;
import com.google.inject.Stage;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;

public class TestNiftyServer
{
    private static final Logger log = LoggerFactory.getLogger(TestNiftyServer.class);
    private NiftyBootstrap bootstrap;
    private int port;

    @BeforeMethod(alwaysRun = true)
    public void setup()
    {
        bootstrap = null;
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws InterruptedException
    {
        if (bootstrap != null) {
            bootstrap.stop();
        }
    }
    private void startServer()
    {
        startServer(getThriftServerDefBuilder());
    }

    private void startServer(final ThriftServerDefBuilder thriftServerDefBuilder)
    {
        bootstrap = Guice.createInjector(Stage.PRODUCTION, new NiftyModule()
        {
            @Override
            protected void configureNifty()
            {
                bind().toInstance(thriftServerDefBuilder.build());
            }
        }).getInstance(NiftyBootstrap.class);

        bootstrap.start();
    }

    private ThriftServerDefBuilder getThriftServerDefBuilder()
    {
        try {
            ServerSocket s = new ServerSocket();
            s.bind(new InetSocketAddress(0));
            port = s.getLocalPort();
            s.close();
        }
        catch (IOException e) {
            port = 8080;
        }

        return new ThriftServerDefBuilder()
                .listen(port)
                .withProcessor(new scribe.Processor<>(new scribe.Iface() {
                    @Override
                    public ResultCode Log(List<LogEntry> messages)
                            throws TException
                    {
                        RequestContext context = RequestContext.getCurrentContext();

                        for (LogEntry message : messages) {
                            log.info("[Client: {}] {}: {}",
                                    context.getRemoteAddress(),
                                    message.getCategory(),
                                    message.getMessage());
                        }
                        return ResultCode.OK;
                    }
                }));
    }

    private scribe.Client makeNiftyClient()
            throws TTransportException, InterruptedException
    {
        InetSocketAddress address = new InetSocketAddress("localhost", port);
        TBinaryProtocol tp = new TBinaryProtocol(new NiftyClient().connectSync(address));
        return new scribe.Client(tp);
    }

    @Test
    public void testBasic() throws InterruptedException, TException
    {
        startServer();
        scribe.Client client1 = makeNiftyClient();
        Assert.assertEquals(client1.Log(Arrays.asList(new LogEntry("client1", "aaa"))), ResultCode.OK);
        Assert.assertEquals(client1.Log(Arrays.asList(new LogEntry("client1", "bbb"))), ResultCode.OK);
        scribe.Client client2 = makeNiftyClient();
        Assert.assertEquals(client2.Log(Arrays.asList(new LogEntry("client2", "ccc"))), ResultCode.OK);
    }

    @Test
    public void testMaxConnections() throws InterruptedException, TException
    {
        startServer(getThriftServerDefBuilder().limitConnectionsTo(1));
        scribe.Client client1 = makeNiftyClient();
        Assert.assertEquals(client1.Log(Arrays.asList(new LogEntry("client1", "aaa"))), ResultCode.OK);
        Assert.assertEquals(client1.Log(Arrays.asList(new LogEntry("client1", "bbb"))), ResultCode.OK);
        scribe.Client client2 = makeNiftyClient();
        try {
            client2.Log(Arrays.asList(new LogEntry("client2", "ccc")));
        } catch (TTransportException e) {
            // expected
        }
    }
}
