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

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.tomcat.jni.SessionTicketKey;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * To make distribution of ticket keys and rotating ticket keys easier, tickets can be distributed
 * as a seed file in the following format
 * {
 *     "current": ["5afc6fb03ba4b15", "9547a3ab68b440ef7"],
 *     "new": ["5afc6fb03ba4b15", "9547a3ab68b440ef7"],
 *     "old": ["5afc6fb03ba4b15", "9547a3ab68b440ef7"]
 * }
 *
 * The real ticket keys are generated from the seeds. The seeds can be arbitrary length hex encoded values.
 * The current seeds are used to generate new tickets, and tickets encrypted with the old and new keys are
 * accepted to allow for some leeway in rotation of tickets.
 *
 * The algorithm used to compute the session ticket encryption keys is the following:
 *
 * aesKey  = hkdf(seed, "aes")  -> truncated to AES bytes.
 * hmacKey = hkdf(seed, "hmac") -> truncated to HMAC bytes
 * name    = hkdf(seed, "name") -> truncated to name bytes
 */
public class TicketSeedParser {

    public static byte[] defaultTicketSalt;
    private static byte[] nameBytes = "name".getBytes();
    private static byte[] aesBytes = "aes".getBytes();
    private static byte[] hmacBytes = "hmac".getBytes();

    static {
        /*
         * Randomly generated salt to use for the purpose of ticket seeds. The salt in the HKDF is meant to be public.
         * We do not want to use a random salt so that every machine that gets the same seed will compute the same
         * ticket keys.
         */
        try {
            defaultTicketSalt = Hex.decodeHex("b78973d13c2d0eb24cf94cd692239867".toCharArray());
        }
        catch (DecoderException e) {
            // This is a fatal error.
            Throwables.propagate(e);
        }
    }

    class SeedSpec {
        @SerializedName("current")
        String[] currentSeeds;
        @SerializedName("new")
        String[] newSeeds;
        @SerializedName("old")
        String[] oldSeeds;
    }

    private static SeedSpec getSeedSpec(File file) throws IOException {
        Gson gson = new Gson();
        try (Reader reader = Files.newReader(file, Charset.defaultCharset())) {
            SeedSpec spec = gson.fromJson(reader, SeedSpec.class);
            return spec;
        } catch (JsonSyntaxException|JsonIOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static SessionTicketKey deriveKeyFromSeed(String seed) throws DecoderException {
        byte[] seedBin = Hex.decodeHex(seed.toCharArray());
        byte[] keyName = CryptoUtil.hkdf(nameBytes, seedBin, defaultTicketSalt, SessionTicketKey.NAME_SIZE);
        byte[] aesKey = CryptoUtil.hkdf(aesBytes, seedBin, defaultTicketSalt, SessionTicketKey.AES_KEY_SIZE);
        byte[] hmacKey = CryptoUtil.hkdf(hmacBytes, seedBin, defaultTicketSalt, SessionTicketKey.HMAC_KEY_SIZE);
        return new SessionTicketKey(keyName, hmacKey, aesKey);
    }

    /**
     * Returns a list of tickets parsed from the ticket file. The keys are returned in a format suitable for use
     * with netty. The first keys are the current keys, following that are the old and new keys.
     */
    public static SessionTicketKey[] parse(File file) throws IOException, DecoderException, IllegalArgumentException {
        SeedSpec spec = getSeedSpec(file);
        if (spec.currentSeeds == null || spec.currentSeeds.length == 0) {
            throw new IllegalArgumentException("current seeds must exist");
        }
        int numSeeds = spec.currentSeeds.length;
        if (spec.newSeeds != null) {
            numSeeds += spec.newSeeds.length;
        }
        if (spec.oldSeeds != null) {
            numSeeds += spec.oldSeeds.length;
        }
        SessionTicketKey[] keys = new SessionTicketKey[numSeeds];
        int idx = 0;
        for (String seed : spec.currentSeeds) {
            keys[idx++] = deriveKeyFromSeed(seed);
        }
        if (spec.newSeeds != null) {
            for (String seed : spec.newSeeds) {
                keys[idx++] = deriveKeyFromSeed(seed);
            }
        }
        if (spec.oldSeeds != null) {
            for (String seed : spec.oldSeeds) {
                keys[idx++] = deriveKeyFromSeed(seed);
            }
        }
        return keys;
    }
}
