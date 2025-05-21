// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.util;

public class VersionUtils {

    /**
     * Compares two version strings. We manually attempt to parse the version strings as a Version object
     * was not introduced until Java 9 and we compile as far back as Java 8.
     */
    public static int compareVersions(String v1, String v2) {
        if (v1 == null && v2 == null) {
            return 0;
        }
        if (v1 == null) {
            return -1;
        }
        if (v2 == null) {
            return 1;
        }

        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");

        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            int p1 = i < parts1.length ? parseVersionPart(parts1[i]) : 0;
            int p2 = i < parts2.length ? parseVersionPart(parts2[i]) : 0;
            if (p1 != p2) {
                return p1 - p2;
            }
        }

        // As a final comparison, compare the raw strings. We're either equal here or dealing with a non-numeric version.
        return v1.compareTo(v2);
    }

    private static int parseVersionPart(String part) {
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            return 0; // fallback for non-numeric parts
        }
    }
}
