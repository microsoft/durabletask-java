// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

/**
 * Utility class for generating the user agent string for the Durable Task SDK.
 */
public final class DurableTaskUserAgentUtil {
    /**
     * The name of the SDK used in the user agent string.
     */
    private static final String SDK_NAME = "durabletask-java";

    private DurableTaskUserAgentUtil() {
        // Private constructor to prevent instantiation
    }

    /**
     * Generates the user agent string for the Durable Task SDK based on a fixed name and the package version.
     *
     * @return The user agent string.
     */
    public static String getUserAgent() {
        return String.format("%s/%s", SDK_NAME, VersionInfo.VERSION);
    }
} 