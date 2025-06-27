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

        // Check if both versions are in standard format (e.g., "1.0.0")
        boolean isV1Standard = isStandardVersionString(v1);
        boolean isV2Standard = isStandardVersionString(v2);
        
        // If both versions were successfully normalized, compare them as structured versions
        if (isV1Standard && isV2Standard) {
            return compareStandardVersions(v1, v2);
        }
        
        // If either version couldn't be normalized, fall back to string comparison
        return v1.compareTo(v2);
    }

    /**
     * Checks if the version string is in a standard format (e.g., "1.0.0").
     * @param version The version string to check
     * @return true if the version is in standard format, false otherwise
     */
    private static boolean isStandardVersionString(String version) {
        if (version == null || version.trim().isEmpty()) {
            return false;
        }
        
        String[] parts = version.split("\\.");
        
        // Check if all parts are numeric
        for (String part : parts) {
            if (tryParseInt(part) == null) {
                return false; // Contains non-numeric part, cannot normalize
            }
        }
        return true;
    }

    /**
     * Compares two standard version strings part by part.
     * @param v1 First standard version string
     * @param v2 Second standard version string
     * @return Negative if v1 < v2, positive if v1 > v2, zero if equal
     */
    private static int compareStandardVersions(String v1, String v2) {
        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");
        
        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            int p1 = i < parts1.length ? Integer.parseInt(parts1[i]) : 0;
            int p2 = i < parts2.length ? Integer.parseInt(parts2[i]) : 0;
            if (p1 != p2) {
                return p1 - p2;
            }
        }
        
        return 0;
    }

    private static Integer tryParseInt(String part) {
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            return null; // indicates non-numeric part
        }
    }
}
