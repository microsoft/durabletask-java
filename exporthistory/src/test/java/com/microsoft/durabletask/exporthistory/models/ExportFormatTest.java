// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExportFormat}.
 */
class ExportFormatTest {

    @Test
    void default_isJsonlWithSchemaVersion1() {
        ExportFormat fmt = ExportFormat.DEFAULT;
        assertEquals(ExportFormatKind.JSONL, fmt.getKind());
        assertEquals("1.0", fmt.getSchemaVersion());
    }

    @Test
    void constructor_validValues() {
        ExportFormat fmt = new ExportFormat(ExportFormatKind.JSON, "2.0");
        assertEquals(ExportFormatKind.JSON, fmt.getKind());
        assertEquals("2.0", fmt.getSchemaVersion());
    }

    @Test
    void constructor_nullKind_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportFormat(null, "1.0"));
    }

    @Test
    void constructor_nullSchemaVersion_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportFormat(ExportFormatKind.JSONL, null));
    }

    @Test
    void constructor_emptySchemaVersion_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportFormat(ExportFormatKind.JSONL, ""));
    }
}
