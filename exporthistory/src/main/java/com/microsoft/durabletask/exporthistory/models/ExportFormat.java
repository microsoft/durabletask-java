// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;

/**
 * Export format settings including format kind and schema version.
 */
public final class ExportFormat {

    /**
     * Default export format: JSONL with schema version 1.0.
     */
    public static final ExportFormat DEFAULT = new ExportFormat(ExportFormatKind.JSONL, "1.0");

    private ExportFormatKind kind;
    private String schemaVersion;

    /**
     * Default constructor for Jackson deserialization.
     */
    public ExportFormat() {
        this.kind = ExportFormatKind.JSONL;
        this.schemaVersion = "1.0";
    }

    /**
     * Creates a new {@code ExportFormat}.
     *
     * @param kind          the output format kind
     * @param schemaVersion the schema version for forward-compatibility
     */
    public ExportFormat(@Nonnull ExportFormatKind kind, @Nonnull String schemaVersion) {
        if (kind == null) {
            throw new IllegalArgumentException("kind must not be null.");
        }
        if (schemaVersion == null || schemaVersion.isEmpty()) {
            throw new IllegalArgumentException("schemaVersion must not be null or empty.");
        }
        this.kind = kind;
        this.schemaVersion = schemaVersion;
    }

    @Nonnull
    public ExportFormatKind getKind() {
        return this.kind;
    }

    @Nonnull
    public String getSchemaVersion() {
        return this.schemaVersion;
    }

    public void setKind(ExportFormatKind kind) { this.kind = kind; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }
}
