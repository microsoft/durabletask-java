// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import java.util.Objects;

/**
 * Export format settings. The default is {@link ExportFormatKind#JSONL} with schema version {@code "1.0"}.
 */
public final class ExportFormat {

    /** The default schema version. */
    public static final String DEFAULT_SCHEMA_VERSION = "1.0";

    private final ExportFormatKind kind;
    private final String schemaVersion;

    /**
     * Creates a new {@code ExportFormat} with the default kind ({@link ExportFormatKind#JSONL}) and
     * schema version ({@value #DEFAULT_SCHEMA_VERSION}).
     */
    public ExportFormat() {
        this(ExportFormatKind.JSONL, DEFAULT_SCHEMA_VERSION);
    }

    /**
     * Creates a new {@code ExportFormat}.
     *
     * @param kind          the export format kind
     * @param schemaVersion the schema version
     */
    public ExportFormat(ExportFormatKind kind, String schemaVersion) {
        this.kind = Objects.requireNonNull(kind, "kind must not be null");
        this.schemaVersion = Objects.requireNonNull(schemaVersion, "schemaVersion must not be null");
    }

    /**
     * Gets the default export format (JSONL with schema version {@value #DEFAULT_SCHEMA_VERSION}).
     *
     * @return the default export format
     */
    public static ExportFormat getDefault() {
        return new ExportFormat();
    }

    /** @return the export format kind. */
    public ExportFormatKind getKind() {
        return this.kind;
    }

    /** @return the schema version. */
    public String getSchemaVersion() {
        return this.schemaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExportFormat)) {
            return false;
        }
        ExportFormat that = (ExportFormat) o;
        return this.kind == that.kind && this.schemaVersion.equals(that.schemaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.kind, this.schemaVersion);
    }
}
