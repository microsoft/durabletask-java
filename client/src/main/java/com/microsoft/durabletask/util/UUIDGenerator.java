package com.microsoft.durabletask.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class UUIDGenerator {
    public static UUID generate(int version, String algorithm, UUID namespace, String name) {

        MessageDigest hasher = hasher(algorithm);

        if (namespace != null) {
            ByteBuffer ns = ByteBuffer.allocate(16);
            ns.putLong(namespace.getMostSignificantBits());
            ns.putLong(namespace.getLeastSignificantBits());
            hasher.update(ns.array());
        }

        hasher.update(name.getBytes(StandardCharsets.UTF_8));
        ByteBuffer hash = ByteBuffer.wrap(hasher.digest());

        final long msb = (hash.getLong() & 0xffffffffffff0fffL) | (version & 0x0f) << 12;
        final long lsb = (hash.getLong() & 0x3fffffffffffffffL) | 0x8000000000000000L;

        return new UUID(msb, lsb);
    }

    private static MessageDigest hasher(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm); /* CodeQL [SM05136] Suppressed: SHA1 is not used for cryptographic purposes here. The information being hashed is not sensitive,
                                                            and the goal is to generate a deterministic Guid. We cannot update to SHA2-based algorithms without breaking
                                                            customers' inflight orchestrations. */
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(String.format("%s not supported.", algorithm));
        }
    }
}
