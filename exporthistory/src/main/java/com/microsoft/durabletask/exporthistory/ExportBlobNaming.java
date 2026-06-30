// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Computes export blob names and paths. The blob name is a SHA-256 hash of
 * {@code "<completedTimestamp ISO-8601>|<instanceId>"} plus the format-specific extension, mirroring the .NET
 * {@code ExportInstanceHistoryActivity} naming scheme.
 */
final class ExportBlobNaming {

    private ExportBlobNaming() {
    }

    /**
     * Builds the blob file name (without any prefix) for an instance export.
     *
     * @param completedTimestamp the instance completion time
     * @param instanceId         the instance ID
     * @param format             the export format
     * @return the blob file name, e.g. {@code "<hex>.jsonl.gz"}
     */
    static String blobFileName(Instant completedTimestamp, String instanceId, ExportFormat format) {
        String hashInput = DateTimeFormatter.ISO_INSTANT.format(completedTimestamp) + "|" + instanceId;
        return sha256Hex(hashInput) + "." + HistoryEventSerializer.fileExtension(format);
    }

    /**
     * Combines an optional prefix with a blob file name.
     *
     * @param prefix   the blob path prefix, or {@code null}/empty for none
     * @param fileName the blob file name
     * @return the full blob path
     */
    static String blobPath(String prefix, String fileName) {
        if (prefix == null || prefix.isEmpty()) {
            return fileName;
        }
        return trimTrailingSlashes(prefix) + "/" + fileName;
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hashBytes.length * 2);
            for (byte b : hashBytes) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available on every JVM.
            throw new IllegalStateException("SHA-256 algorithm not available.", e);
        }
    }

    private static String trimTrailingSlashes(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }
}
