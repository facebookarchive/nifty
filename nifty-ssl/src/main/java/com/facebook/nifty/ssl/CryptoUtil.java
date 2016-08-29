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
package com.facebook.nifty.ssl;

import org.apache.commons.codec.digest.HmacUtils;

import javax.crypto.Mac;

public class CryptoUtil {

    public static final int SHA2_BYTES = 32;
    private static final int MAX_OUTPUT_LENGTH = SHA2_BYTES * 255;
    private static final byte[] nullSalt = new byte[SHA2_BYTES];

    private static byte[] hmacHash(byte[] prk, byte[] data) {
        Mac mac = HmacUtils.getHmacSha256(prk);
        return mac.doFinal(data);
    }

    private static byte[] hmacHash(byte[] prk, byte[] data, byte[] info, byte count) {
        Mac mac = HmacUtils.getHmacSha256(prk);
        if (data != null) {
            mac.update(data);
        }
        mac.update(info);
        mac.update(count);
        return mac.doFinal();
    }

    public static byte[] hkdf(byte[] info, byte[] key, byte[] salt, int outputLength) {
        if (outputLength > MAX_OUTPUT_LENGTH) {
            throw new IllegalArgumentException("Output length too large " + outputLength);
        }

        if (salt == null) {
            salt = nullSalt;
        }
        byte[] prk = hmacHash(salt, key);

        int N = (int) Math.ceil(outputLength / SHA2_BYTES);
        byte[] outputData = new byte[outputLength];

        int idx = 0;
        byte[] current = null;

        for (int i = 1; i <= N + 1; ++i) {
            byte counter = (byte) i;
            current = hmacHash(prk, current, info, counter);
            System.arraycopy(current, 0, outputData, idx, Math.min(SHA2_BYTES, outputLength - idx));
            idx += SHA2_BYTES;
        }
        return outputData;
    }
}
