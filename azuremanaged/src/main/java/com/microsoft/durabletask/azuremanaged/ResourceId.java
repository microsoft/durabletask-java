// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

/**
 * Resolves token audiences at configuration boundaries, before scopes are created.
 */
final class ResourceId {
    private static final String DEFAULT_SCOPE_SUFFIX = "/.default";

    private ResourceId() {
    }

    static String getDefault() {
        return getDefault(System.getenv("REGION_NAME"));
    }

    static String getDefault(String regionName) {
        if (regionName != null
                && (regionName.regionMatches(true, 0, "usgov", 0, 5)
                    || regionName.regionMatches(true, 0, "usdod", 0, 5))) {
            return "https://durabletask.azure.us";
        }
        return "https://durabletask.io";
    }

    static String resolve(String resourceId) {
        if (resourceId == null || resourceId.isEmpty()) {
            return getDefault();
        }

        String normalized = trimTrailingSlashes(resourceId.trim());
        if (normalized.regionMatches(true, normalized.length() - DEFAULT_SCOPE_SUFFIX.length(),
                DEFAULT_SCOPE_SUFFIX, 0, DEFAULT_SCOPE_SUFFIX.length())) {
            normalized = trimTrailingSlashes(
                normalized.substring(0, normalized.length() - DEFAULT_SCOPE_SUFFIX.length()));
        }
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(
                "ResourceId must not be empty after normalization. Specify a token audience URI, "
                    + "or omit ResourceId to use the region-based default.");
        }
        return normalized;
    }

    private static String trimTrailingSlashes(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }
}
