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
package com.facebook.nifty.duplex;

import org.apache.thrift.protocol.TProtocol;

/***
 * An interface for representing a pair of protocols: one for input and one for output,
 * and utility functions for creating a pair instance from a single protocol or from
 * separate input and output protocols.
 */
public abstract class TProtocolPair {
    public abstract TProtocol getInputProtocol();
    public abstract TProtocol getOutputProtocol();

    public static TProtocolPair fromSeparateProtocols(final TProtocol inputProtocol,
                                                      final TProtocol outputProtocol) {
        return new TProtocolPair() {
            @Override
            public TProtocol getInputProtocol() {
                return inputProtocol;
            }

            @Override
            public TProtocol getOutputProtocol() {
                return outputProtocol;
            }
        };
    }

    public static TProtocolPair fromSingleProtocol(final TProtocol protocol) {
        return new TProtocolPair() {
            @Override
            public TProtocol getInputProtocol() {
                return protocol;
            }

            @Override
            public TProtocol getOutputProtocol() {
                return protocol;
            }
        };
    }
}
