// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

/**
 * Output format kind for exported history.
 */
public enum ExportFormatKind {
    /**
     * JSONL format — one history event per line, gzip compressed.
     */
    JSONL,

    /**
     * JSON format — JSON array of history events, uncompressed.
     */
    JSON
}
