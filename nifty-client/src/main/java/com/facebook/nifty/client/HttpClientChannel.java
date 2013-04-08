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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.Timer;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;

@NotThreadSafe
public class HttpClientChannel extends AbstractClientChannel {
    private final Channel underlyingNettyChannel;
    private final String hostName;
    private Map<String, String> headerDictionary;
    private final String endpointUri;

    public HttpClientChannel(Channel channel,
                             Timer timer,
                             String hostName,
                             String endpointUri) {
        super(channel, timer);

        this.underlyingNettyChannel = channel;
        this.hostName = hostName;
        this.endpointUri = endpointUri;
    }

    @Override
    protected int extractSequenceId(ChannelBuffer message)
            throws TTransportException
    {
        try {
            int sequenceId;
            int stringLength;
            stringLength = message.getInt(4);
            sequenceId = message.getInt(8 + stringLength);
            return sequenceId;
        } catch (Throwable t) {
            throw new TTransportException("Could not find sequenceId in Thrift message");
        }
    }

    @Override
    public Channel getNettyChannel() {
        return underlyingNettyChannel;
    }

    @Override
    protected ChannelBuffer extractResponse(Object message) throws TTransportException
    {
        if (!(message instanceof HttpResponse)) {
            return null;
        }

        HttpResponse httpResponse = (HttpResponse) message;

        if (!httpResponse.getStatus().equals(HttpResponseStatus.OK)) {
            throw new TTransportException("HTTP response had non-OK status: " + httpResponse
                    .getStatus().toString());
        }

        ChannelBuffer content = httpResponse.getContent();

        if (!content.readable()) {
            return null;
        }

        return content;
    }

    @Override
    protected ChannelFuture writeRequest(ChannelBuffer request)
    {
        HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                         endpointUri);

        httpRequest.setHeader(HttpHeaders.HOST, hostName);
        httpRequest.setHeader(HttpHeaders.CONTENT_LENGTH, request.readableBytes());
        httpRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-thrift");
        httpRequest.setHeader(HttpHeaders.ACCEPT, "application/x-thrift");
        httpRequest.setHeader(HttpHeaders.USER_AGENT, "Java/Swift-HttpThriftClientChannel");

        if (headerDictionary != null) {
            for (Map.Entry<String, String> entry : headerDictionary.entrySet()) {
                httpRequest.setHeader(entry.getKey(), entry.getValue());
            }
        }

        httpRequest.setContent(request);

        return underlyingNettyChannel.write(httpRequest);
    }

    public void setHeaders(Map<String, String> headers)
    {
        this.headerDictionary = ImmutableMap.copyOf(headers);
    }

    public ImmutableMap<String, String> getHeaders()
    {
        return ImmutableMap.copyOf(this.headerDictionary);
    }

}
