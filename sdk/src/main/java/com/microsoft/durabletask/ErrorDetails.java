// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ErrorDetails {
    
    private String fullText;

    public ErrorDetails() {
    }
    
    public ErrorDetails(Exception e) {
        this.fullText = getFullStackTrace(e);
    }

    public String getFullText() {
        return this.fullText;
    }

    void setFullText(String fullText) {
        this.fullText = fullText;
    }

    static String getFullStackTrace(Exception e) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer );
        e.printStackTrace(printWriter);
        printWriter.flush();
        return writer.toString();
    }
}