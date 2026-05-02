// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExportDestination}.
 */
class ExportDestinationTest {

    @Test
    void constructor_validContainer() {
        ExportDestination dest = new ExportDestination("my-container");
        assertEquals("my-container", dest.getContainer());
        assertNull(dest.getPrefix());
    }

    @Test
    void constructor_validContainerAndPrefix() {
        ExportDestination dest = new ExportDestination("my-container", "exports/");
        assertEquals("my-container", dest.getContainer());
        assertEquals("exports/", dest.getPrefix());
    }

    @Test
    void constructor_nullContainer_throws() {
        assertThrows(IllegalArgumentException.class, () -> new ExportDestination(null));
    }

    @Test
    void constructor_emptyContainer_throws() {
        assertThrows(IllegalArgumentException.class, () -> new ExportDestination(""));
    }
}
