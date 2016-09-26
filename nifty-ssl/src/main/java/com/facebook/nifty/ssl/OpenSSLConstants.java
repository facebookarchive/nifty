package com.facebook.nifty.ssl;

/**
 * Collection of constants defined by OpenSSL which are not available via
 * netty tc-native.
 */
public interface OpenSSLConstants {
    /* Session related */
    long SSL_SESS_CACHE_NO_INTERNAL_LOOKUP = 0x0100;
    long SSL_SESS_CACHE_NO_INTERNAL_STORE = 0x0200;
    long SSL_SESS_CACHE_NO_INTERNAL = SSL_SESS_CACHE_NO_INTERNAL_LOOKUP | SSL_SESS_CACHE_NO_INTERNAL_STORE;
}
