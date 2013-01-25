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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;

public class TestHttpClientConnector {
  @Test
  public void testSchemeCheck() {
    try {
      new HttpClientConnector(URI.create("http://good.com/foo"));
    } catch (Exception e) {
      Assert.fail("unexpected exception", e);
    }
    try {
      new HttpClientConnector(URI.create("HTTP://good.com/foo"));
    } catch (Exception e) {
      Assert.fail("unexpected exception", e);
    }
    try {
      new HttpClientConnector(URI.create("https://good.com/foo"));
    } catch (Exception e) {
      Assert.fail("unexpected exception", e);
    }
    try {
      new HttpClientConnector(URI.create("http://good.com:8000/foo"));
    } catch (Exception e) {
      Assert.fail("unexpected exception", e);
    }
    try {
      new HttpClientConnector(URI.create("https://good.com:80/foo"));
    } catch (Exception e) {
      Assert.fail("unexpected exception", e);
    }
    try {
      new HttpClientConnector(URI.create("https://good.com:8888888/foo"));
      Assert.fail("expected exception but got here instead");
    } catch (Exception e) {
    }
    try {
      new HttpClientConnector(URI.create("ftp://good.com/foo"));
      Assert.fail("expected exception but got here instead");
    } catch (Exception e) {
    }
  }
}
