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

        // We're looking for a dot-separated version string, e.g. "1.0.0".
        // This can contain strings and falls back to string comparison if that is the case.
        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");

        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            String part1 = i < parts1.length ? parts1[i] : "";
            String part2 = i < parts2.length ? parts2[i] : "";
            
            // Try to parse as integers first
            Integer num1 = tryParseInt(part1);
            Integer num2 = tryParseInt(part2);
            
            if (num1 != null && num2 != null) {
                // Both are numeric, compare as integers
                if (!num1.equals(num2)) {
                    return num1 - num2;
                }
            } else if (num1 != null) {
                // part1 is numeric, part2 is not - numeric versions come before non-numeric
                return 1;
            } else if (num2 != null) {
                // part2 is numeric, part1 is not - numeric versions come before non-numeric
                return -1;
            } else {
                // Both are non-numeric, compare as strings
                int stringComparison = part1.compareTo(part2);
                if (stringComparison != 0) {
                    return stringComparison;
                }
            }
        }

        return 0; // All parts are equal
    }

    private static Integer tryParseInt(String part) {
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            return null; // indicates non-numeric part
        }
    }
}
