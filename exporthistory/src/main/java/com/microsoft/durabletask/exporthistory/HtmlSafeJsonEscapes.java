// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.io.SerializedString;

/**
 * Character escaping that reproduces the export wire format's HTML-safe encoder byte-for-byte.
 * <p>
 * Escaping rules (paired with {@code JsonGenerator.setHighestNonEscapedChar(0x7F)} so every character at or above
 * {@code U+0080} is escaped):
 * <ul>
 *   <li>{@code " & ' + < > `} and {@code DEL (0x7F)} are written as {@code \\uXXXX} (uppercase hex), not the JSON
 *       short forms.</li>
 *   <li>{@code \\ \b \t \n \f \r} keep their JSON short forms.</li>
 *   <li>Other control characters below {@code 0x20} are written as {@code \\uXXXX}.</li>
 *   <li>Every character at or above {@code U+0080} (including surrogate-pair halves) is written as {@code \\uXXXX}
 *       (uppercase hex).</li>
 * </ul>
 */
final class HtmlSafeJsonEscapes extends CharacterEscapes {

    private static final long serialVersionUID = 1L;

    /** Characters that must be emitted as {@code \\uXXXX} instead of their JSON short escape. */
    private static final int[] FORCED_UNICODE = {'"', '&', '\'', '+', '<', '>', '`', 0x7F};

    private final int[] asciiEscapes;

    HtmlSafeJsonEscapes() {
        int[] esc = CharacterEscapes.standardAsciiEscapesForJSON();
        for (int c : FORCED_UNICODE) {
            esc[c] = CharacterEscapes.ESCAPE_CUSTOM;
        }
        this.asciiEscapes = esc;
    }

    @Override
    public int[] getEscapeCodesForAscii() {
        return this.asciiEscapes;
    }

    @Override
    public SerializableString getEscapeSequence(int ch) {
        return new SerializedString(String.format("\\u%04X", ch));
    }
}
