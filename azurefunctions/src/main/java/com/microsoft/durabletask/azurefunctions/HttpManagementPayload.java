/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions;

/**
 * The class that holds the webhook URLs to manage orchestration instances.
 * The Functions host controls whether these URLs include a system key.
 * When {@code httpSettings.excludeManagementKeyFromUrls} is enabled, callers
 * must obtain a key separately and send it in the {@code x-functions-key} request header.
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

    /**
     * Creates a {@link HttpManagementPayload} to manage orchestration instances
     *
     * @param instanceId The ID of the orchestration instance
     * @param instanceStatusURL The base webhook url of the orchestration instance
     * @param requiredQueryStringParameters The query parameters supplied by the Functions host, preserved unchanged
     */
    public HttpManagementPayload(
            String instanceId,
            String instanceStatusURL,
            String requiredQueryStringParameters) {
        String query = requiredQueryStringParameters == null ? "" : requiredQueryStringParameters;
        if (query.startsWith("?")) {
            query = query.substring(1);
        }

        String querySuffix = query.isEmpty() ? "" : "?" + query;
        String reasonSuffix = "?reason={text}" + (query.isEmpty() ? "" : "&" + query);
        this.id = instanceId;
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
