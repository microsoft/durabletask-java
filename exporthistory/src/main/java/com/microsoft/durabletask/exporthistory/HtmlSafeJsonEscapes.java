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

    /** Shared, immutable instance -- the escape table and rules are stateless. */
    static final HtmlSafeJsonEscapes INSTANCE = new HtmlSafeJsonEscapes();

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    /** Characters that must be emitted as {@code \\uXXXX} instead of their JSON short escape. */
    private static final int[] FORCED_UNICODE = {'"', '&', '\'', '+', '<', '>', '`', 0x7F};

    /** Precomputed once and shared across all events; callers receive a defensive copy. */
    private static final int[] ASCII_ESCAPES = buildAsciiEscapes();

    private HtmlSafeJsonEscapes() {
    }

    private static int[] buildAsciiEscapes() {
        int[] esc = CharacterEscapes.standardAsciiEscapesForJSON();
        for (int c : FORCED_UNICODE) {
            esc[c] = CharacterEscapes.ESCAPE_CUSTOM;
        }
        return esc;
    }

    @Override
    public int[] getEscapeCodesForAscii() {
        return ASCII_ESCAPES.clone();
    }

    @Override
    public SerializableString getEscapeSequence(int ch) {
        char[] buf = {'\\', 'u', HEX[(ch >> 12) & 0xF], HEX[(ch >> 8) & 0xF], HEX[(ch >> 4) & 0xF], HEX[ch & 0xF]};
        return new SerializedString(new String(buf));
    }
}
