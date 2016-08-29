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

import org.apache.commons.codec.DecoderException;
import org.apache.tomcat.jni.SessionTicketKey;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TicketSeedParserTest {

    @Test
    public void testParseSeeds() throws IOException, DecoderException {
        SessionTicketKey[] keys =
                TicketSeedParser.parse(
                        new File(TicketSeedParserTest.class.getResource("/good_seeds.json").getFile()));
        Assert.assertEquals(6, keys.length);

        // The seeds in the file are arranged so that every 2 are the same, and adjacent ones are not.
        for (int i = 0; i < keys.length / 2; i += 2) {
            Assert.assertTrue(Arrays.equals(keys[i].getHmacKey(), keys[i + 2].getHmacKey()));
            Assert.assertTrue(Arrays.equals(keys[i].getName(), keys[i + 2].getName()));
            Assert.assertTrue(Arrays.equals(keys[i].getAesKey(), keys[i + 2].getAesKey()));
        }
        for (int i = 0; i < keys.length - 1; i++) {
            Assert.assertNotEquals(keys[i].getHmacKey(), keys[i + 1].getHmacKey());
            Assert.assertNotEquals(keys[i].getName(), keys[i + 1].getName());
            Assert.assertNotEquals(keys[i].getAesKey(), keys[i + 1].getAesKey());
        }

        SessionTicketKey[] keys2 =
                TicketSeedParser.parse(
                        new File(TicketSeedParserTest.class.getResource("/good_seeds.json").getFile()));
        Assert.assertEquals(keys.length, keys2.length);
        for (int i = 0; i < keys.length; ++i) {
            Assert.assertEquals(keys[i].getAesKey(), keys2[i].getAesKey(), "AES key not equal");
            Assert.assertEquals(keys[i].getName(), keys2[i].getName());
            Assert.assertEquals(keys[i].getHmacKey(), keys2[i].getHmacKey());
        }
    }

    @Test
    public void testParseCurrentSeeds() throws IOException, DecoderException {
        SessionTicketKey[] keys =
                TicketSeedParser.parse(
                        new File(TicketSeedParserTest.class.getResource("/seeds_with_only_current.json").getFile()));
        Assert.assertEquals(2, keys.length);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseNoCurrentSeeds() throws IOException, DecoderException {
        TicketSeedParser.parse(
                new File(TicketSeedParserTest.class.getResource("/seeds_with_no_current.json").getFile()));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseBadJson() throws IOException, DecoderException {
        TicketSeedParser.parse(
                new File(TicketSeedParserTest.class.getResource("/seeds_bad_json.json").getFile()));
    }
}
