/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The class that holds the webhook URLs to manage orchestration instances.
 */
public class HttpManagementPayload {
    private final String id;
    private final String purgeHistoryDeleteUri;
    private final String restartPostUri;
    private final String rewindPostUri;
    private final String sendEventPostUri;
    private final String statusQueryGetUri;
    private final String terminatePostUri;
    private final String resumePostUri;
    private final String suspendPostUri;
    private final Map<String, String> requiredHeaders;

    /**
     * Creates a {@link HttpManagementPayload} to manage orchestration instances
     *
     * @param instanceId The ID of the orchestration instance
     * @param instanceStatusURL The base webhook url of the orchestration instance
     * @param requiredQueryStringParameters The query parameters to include with the http request
     */
    public HttpManagementPayload(
            String instanceId,
            String instanceStatusURL,
            String requiredQueryStringParameters) {
        this(instanceId, instanceStatusURL, requiredQueryStringParameters, Collections.emptyMap());
    }

    /**
     * Creates management URLs with authentication supplied separately from the query string.
     *
     * @param instanceId The ID of the orchestration instance
     * @param instanceStatusURL The base webhook URL of the orchestration instance
     * @param requiredQueryStringParameters The non-secret query parameters to include with requests
     * @param requiredHeaders The headers callers must send when using the management URLs
     */
    public HttpManagementPayload(
            String instanceId,
            String instanceStatusURL,
            String requiredQueryStringParameters,
            Map<String, String> requiredHeaders) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (requiredHeaders != null) {
            requiredHeaders.forEach((name, value) -> headers.put(name.toLowerCase(Locale.ROOT), value));
        }

        List<String> parameters = new ArrayList<>();
        String query = requiredQueryStringParameters == null ? "" : requiredQueryStringParameters;
        if (query.startsWith("?")) {
            query = query.substring(1);
        }

        // Older hosts include the system key in the binding query string.
        for (String parameter : query.split("&")) {
            if (parameter.isEmpty()) {
                continue;
            }

            int separator = parameter.indexOf('=');
            String name = separator < 0 ? parameter : parameter.substring(0, separator);
            if (decode(name).equalsIgnoreCase("code")) {
                if (separator >= 0) {
                    headers.putIfAbsent("x-functions-key", decode(parameter.substring(separator + 1)));
                }
            } else {
                parameters.add(parameter);
            }
        }

        String queryString = String.join("&", parameters);
        String querySuffix = queryString.isEmpty() ? "" : "?" + queryString;
        String reasonSuffix = "?reason={text}" + (queryString.isEmpty() ? "" : "&" + queryString);
        this.id = instanceId;
        this.requiredHeaders = Collections.unmodifiableMap(headers);
        this.purgeHistoryDeleteUri = instanceStatusURL + querySuffix;
        this.restartPostUri = instanceStatusURL + "/restart" + querySuffix;
        this.rewindPostUri = instanceStatusURL + "/rewind" + reasonSuffix;
        this.sendEventPostUri = instanceStatusURL + "/raiseEvent/{eventName}" + querySuffix;
        this.statusQueryGetUri = instanceStatusURL + querySuffix;
        this.terminatePostUri = instanceStatusURL + "/terminate" + reasonSuffix;
        this.resumePostUri = instanceStatusURL + "/resume" + reasonSuffix;
        this.suspendPostUri = instanceStatusURL + "/suspend" + reasonSuffix;
    }

    /**
     * Gets the headers callers must send when using the management URLs.
     * Treat these values as secrets; do not log them or append them to URLs.
     *
     * @return An immutable map of required HTTP request headers.
     */
    public Map<String, String> getRequiredHeaders() {
        return this.requiredHeaders;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("UTF-8 encoding is unavailable.", ex);
        }
    }

    /**
     * Gets the ID of the orchestration instance.
     *
     * @return The ID of the orchestration instance.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Gets the HTTP GET status query endpoint URL.
     *
     * @return The HTTP URL for fetching the instance status.
     */
    public String getStatusQueryGetUri() {
        return this.statusQueryGetUri;
    }

    /**
     * Gets the HTTP POST external event sending endpoint URL.
     *
     * @return The HTTP URL for posting external event notifications.
     */
    public String getSendEventPostUri() {
        return this.sendEventPostUri;
    }

    /**
     * Gets the HTTP POST instance termination endpoint.
     *
     * @return The HTTP URL for posting instance termination commands.
     */
    public String getTerminatePostUri() {
        return this.terminatePostUri;
    }

    /**
     * Gets the HTTP DELETE purge instance history by instance ID endpoint.
     *
     * @return The HTTP URL for purging instance history by instance ID.
     */
    public String getPurgeHistoryDeleteUri() {
        return this.purgeHistoryDeleteUri;
    }

    /**
     * Gets the HTTP POST instance restart endpoint.
     *
     * @return The HTTP URL for posting instance restart commands.
     */
    public String getRestartPostUri() {
        return this.restartPostUri;
    }

    /**
     * Gets the HTTP POST instance suspend endpoint.
     *
     * @return The HTTP URL for posting instance suspend commands.
     */
    public String getSuspendPostUri() {
        return this.suspendPostUri;
    }

    /**
     * Gets the HTTP POST instance resume endpoint.
     *
     * @return The HTTP URL for posting instance resume commands.
     */
    public String getResumePostUri() {
        return this.resumePostUri;
    }

    /**
     * Gets the HTTP POST instance rewind endpoint.
     *
     * @return The HTTP URL for posting instance rewind commands.
     */
    public String getRewindPostUri() {
        return this.rewindPostUri;
    }

}
