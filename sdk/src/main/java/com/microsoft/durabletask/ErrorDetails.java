// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.fasterxml.jackson.annotation.*;

// NOTE: This type is serializable and represents a strict error contract for Durable Tasks
//       across multiple languages.
class ErrorDetails {
    private final String errorName;
    private final String errorMessage;
    private final String errorDetails;

    @JsonCreator
    public ErrorDetails(
            @JsonProperty("errorName") String errorName,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("errorDetails") String errorDetails) {
        this.errorName = errorName;
        this.errorMessage = errorMessage;
        this.errorDetails = errorDetails;
    }

    public ErrorDetails(Exception exception) {
        this(exception.getClass().getName(), exception.getMessage(), getFullStackTrace(exception));
    }

    @JsonGetter("errorName")
    public String getErrorName() {
        return this.errorName;
    }

    @JsonGetter("errorMessage")
    public String getErrorMessage() {
        return this.errorMessage;
    }

    @JsonGetter("errorDetails")
    public String getErrorDetails() {
        return this.errorDetails;
    }

    static String getFullStackTrace(Throwable e) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer );
        e.printStackTrace(printWriter);
        printWriter.flush();
        return writer.toString();
    }
}