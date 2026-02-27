/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions;

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
        this.id = instanceId;
        this.purgeHistoryDeleteUri = instanceStatusURL + "?" + requiredQueryStringParameters;
        this.restartPostUri = instanceStatusURL + "/restart?" + requiredQueryStringParameters;
        this.rewindPostUri = instanceStatusURL + "/rewind?reason={text}&" + requiredQueryStringParameters;
        this.sendEventPostUri = instanceStatusURL + "/raiseEvent/{eventName}?" + requiredQueryStringParameters;
        this.statusQueryGetUri = instanceStatusURL + "?" + requiredQueryStringParameters;
        this.terminatePostUri = instanceStatusURL + "/terminate?reason={text}&" + requiredQueryStringParameters;
        this.resumePostUri = instanceStatusURL + "/resume?reason={text}&" + requiredQueryStringParameters;
        this.suspendPostUri = instanceStatusURL + "/suspend?reason={text}&" + requiredQueryStringParameters;
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
        return restartPostUri;
    }

    /**
     * Gets the HTTP POST instance rewind endpoint.
     *
     * @return The HTTP URL for posting instance rewind commands.
     */
    public String getRewindPostUri() {
        return rewindPostUri;
    }

}
