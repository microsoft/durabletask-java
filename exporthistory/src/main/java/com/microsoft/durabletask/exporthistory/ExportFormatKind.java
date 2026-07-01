// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * The kind of export format.
 */
public enum ExportFormatKind {
    /** JSONL format (one history event per line, compressed with gzip). */
    JSONL,

    /** JSON format (array of history events, uncompressed). */
    JSON
}
