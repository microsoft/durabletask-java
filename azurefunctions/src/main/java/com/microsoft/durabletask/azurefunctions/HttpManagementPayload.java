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
    private final String sendEventPostUri;
    private final String statusQueryGetUri;
    private final String terminatePostUri;

    public HttpManagementPayload(
            String instanceId,
            String instanceStatusURL,
            String requiredQueryStringParameters) {
        this.id = instanceId;
        this.purgeHistoryDeleteUri = instanceStatusURL + "?" + requiredQueryStringParameters;
        this.sendEventPostUri = instanceStatusURL + "/raiseEvent/{eventName}?" + requiredQueryStringParameters;
        this.statusQueryGetUri = instanceStatusURL + "?" + requiredQueryStringParameters;
        this.terminatePostUri = instanceStatusURL + "/terminate?reason={text}&" + requiredQueryStringParameters;
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
}
