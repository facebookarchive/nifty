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
package com.facebook.nifty.core;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class RequestContexts
{
    private static final String REQUEST_SUCCEEDED = "request_succeeded";
    private static final String TRACING_INFO = "tracing_info";
    private static ThreadLocal<RequestContext> threadLocalContext = new ThreadLocal<>();

    private RequestContexts()
    {
    }

    /**
     * Gets the thread-local {@link NiftyRequestContext} for the Thrift request that is being processed
     * on the current thread.
     *
     * @return The {@link NiftyRequestContext} of the current request
     */
    public static RequestContext getCurrentContext()
    {
        RequestContext currentContext = threadLocalContext.get();
        return currentContext;
    }

    /**
     * Sets the thread-local context for the currently running request.
     *
     * This is normally called only by the server, but it can also be useful to call when
     * dispatching to another thread (e.g. a thread in an ExecutorService) if the code that will
     * run on that thread might also be interested in the {@link RequestContext}
     */
    public static void setCurrentContext(RequestContext requestContext)
    {
        threadLocalContext.set(requestContext);
    }

    /**
     * Gets the thread-local context for the currently running request
     *
     * This is normally called only by the server, but it can also be useful to call when
     * cleaning up a context
     */
    public static void clearCurrentContext()
    {
        threadLocalContext.remove();
    }

    /**
     * Convenient getter/setter/clearer methods for request succeeded.
     */
    public static boolean getRequestSucceeded() {
        RequestContext currentContext = getCurrentContext();
        if (currentContext == null || currentContext.getContextData(REQUEST_SUCCEEDED) == null) {
            // Default to true if this isn't set explicitly
            return true;
        }

        return ((Boolean)currentContext.getContextData(REQUEST_SUCCEEDED)).booleanValue();
    }

    public static void setRequestSucceeded(boolean succeeded) {
        RequestContext currentContext = getCurrentContext();
        if (currentContext == null) {
            return;
        }

        currentContext.setContextData(REQUEST_SUCCEEDED, Boolean.valueOf(succeeded));
    }

    public static void clearRequestSucceeded() {
        RequestContext currentContext = getCurrentContext();
        if (currentContext == null) {
            return;
        }

        currentContext.clearContextData(REQUEST_SUCCEEDED);
    }

    /**
     * Convenient getter/setter/clearer methods for application tracing info.
     */

    public static Map<String, String> getTracingInfo() {
        RequestContext currentContext = getCurrentContext();
        if (currentContext == null || currentContext.getContextData(TRACING_INFO) == null) {
            return ImmutableMap.of();
        }

        return (Map<String, String>)currentContext.getContextData(TRACING_INFO);
    }

    public static void setTracingInfo(Map<String, String> tracingInfo) {
        RequestContext currentContext = getCurrentContext();
        if (currentContext == null) {
            return;
        }

        currentContext.setContextData(TRACING_INFO, tracingInfo);
    }

    public static void clearTracingInfo() {
        RequestContext currentContext = getCurrentContext();
        if (currentContext == null) {
            return;
        }

        currentContext.clearContextData(TRACING_INFO);
    }
}
