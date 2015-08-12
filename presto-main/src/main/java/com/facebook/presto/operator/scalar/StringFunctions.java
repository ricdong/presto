/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.InvalidCodePointException;
import io.airlift.slice.InvalidUtf8Exception;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointSafe;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.SliceUtf8.toLowerCase;
import static io.airlift.slice.SliceUtf8.toUpperCase;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.SURROGATE;

/**
 * Current implementation is based on code points from Unicode and does ignore grapheme cluster boundaries.
 * Therefore only some methods work correctly with grapheme cluster boundaries.
 */
public final class StringFunctions
{
    private StringFunctions() {}

    @Description("convert Unicode code point to a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice chr(@SqlType(StandardTypes.BIGINT) long codepoint)
    {
        try {
            return SliceUtf8.codePointToUtf8(Ints.saturatedCast(codepoint));
        }
        catch (InvalidCodePointException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a valid Unicode code point: " + codepoint, e);
        }
    }

    @Description("count of code points of the given string")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long length(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return countCodePoints(slice);
    }

    @Description("greedily removes occurrences of a pattern in a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice replace(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.VARCHAR) Slice search)
    {
        return replace(str, search, Slices.EMPTY_SLICE);
    }

    @Description("greedily replaces occurrences of a pattern with a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice replace(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.VARCHAR) Slice search, @SqlType(StandardTypes.VARCHAR) Slice replace)
    {
        // Empty search?
        if (search.length() == 0) {
            // With empty `search` we insert `replace` in front of every character and and the end
            Slice buffer = Slices.allocate((countCodePoints(str) + 1) * replace.length() + str.length());
            // Always start with replace
            buffer.setBytes(0, replace);
            int indexBuffer = replace.length();
            // After every code point insert `replace`
            int index = 0;
            while (index < str.length()) {
                int codePointLength = lengthOfCodePointSafe(str, index);
                // Append current code point
                buffer.setBytes(indexBuffer, str, index, codePointLength);
                indexBuffer += codePointLength;
                // Append `replace`
                buffer.setBytes(indexBuffer, replace);
                indexBuffer += replace.length();
                // Advance pointer to current code point
                index += codePointLength;
            }

            return buffer;
        }
        // Allocate a reasonable buffer
        Slice buffer = Slices.allocate(str.length());

        int index = 0;
        int indexBuffer = 0;
        while (index < str.length()) {
            int matchIndex = str.indexOf(search, index);
            // Found a match?
            if (matchIndex < 0) {
                // No match found so copy the rest of string
                int bytesToCopy = str.length() - index;
                buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy);
                buffer.setBytes(indexBuffer, str, index, bytesToCopy);
                indexBuffer += bytesToCopy;

                break;
            }

            int bytesToCopy = matchIndex - index;
            buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy + replace.length());
            // Non empty match?
            if (bytesToCopy > 0) {
                buffer.setBytes(indexBuffer, str, index, bytesToCopy);
                indexBuffer += bytesToCopy;
            }
            // Non empty replace?
            if (replace.length() > 0) {
                buffer.setBytes(indexBuffer, replace);
                indexBuffer += replace.length();
            }
            // Continue searching after match
            index = matchIndex + search.length();
        }

        return buffer.slice(0, indexBuffer);
    }

    @Description("reverse all code points in a given string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice reverse(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return SliceUtf8.reverse(slice);
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        if (substring.length() == 0) {
            return 1;
        }

        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return countCodePoints(string, 0, index) + 1;
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice substr(@SqlType(StandardTypes.VARCHAR) Slice utf8, @SqlType(StandardTypes.BIGINT) long start)
    {
        if ((start == 0) || utf8.length() == 0) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = utf8.length();

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd = utf8.length();

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice substr(@SqlType(StandardTypes.VARCHAR) Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        if (start == 0 || (length <= 0) || (utf8.length() == 0)) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);
        int lengthCodePoints = Ints.saturatedCast(length);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
            if (indexEnd < 0) {
                // after end of string
                indexEnd = utf8.length();
            }

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd;
        if (startCodePoint + lengthCodePoints < codePoints) {
            indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
        }
        else {
            indexEnd = utf8.length();
        }

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

    @ScalarFunction
    @SqlType("array<varchar>")
    public static Block split(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice delimiter)
    {
        return split(string, delimiter, string.length() + 1);
    }

    @ScalarFunction
    @SqlType("array<varchar>")
    public static Block split(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice delimiter, @SqlType(StandardTypes.BIGINT) long limit)
    {
        checkCondition(limit > 0, INVALID_FUNCTION_ARGUMENT, "Limit must be positive");
        checkCondition(limit <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "Limit is too large");
        checkCondition(delimiter.length() > 0, INVALID_FUNCTION_ARGUMENT, "The delimiter may not be the empty string");
        // If limit is one, the last and only element is the complete string
        if (limit == 1) {
            return toStackRepresentation(ImmutableList.of(string), VARCHAR);
        }

        // todo this should write directly into a block
        List<Slice> parts = new ArrayList<>();

        int index = 0;
        while (index < string.length()) {
            int splitIndex = string.indexOf(delimiter, index);
            // Found split?
            if (splitIndex < 0) {
                break;
            }
            // Add the part from current index to found split
            parts.add(string.slice(index, splitIndex - index));
            // Continue searching after delimiter
            index = splitIndex + delimiter.length();
            // Reached limit-1 parts so we can stop
            if (parts.size() == limit - 1) {
                break;
            }
        }
        // Rest of string
        parts.add(string.slice(index, string.length() - index));

        return toStackRepresentation(parts, VARCHAR);
    }

    @Nullable
    @Description("splits a string by a delimiter and returns the specified field (counting from one)")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice splitPart(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice delimiter, @SqlType(StandardTypes.BIGINT) long index)
    {
        checkCondition(index > 0, INVALID_FUNCTION_ARGUMENT, "Index must be greater than zero");
        // Empty delimiter? Then every character will be a split
        if (delimiter.length() == 0) {
            int startCodePoint = Ints.checkedCast(index);

            int indexStart = offsetOfCodePoint(string, startCodePoint - 1);
            if (indexStart < 0) {
                // index too big
                return null;
            }
            int length = lengthOfCodePoint(string, indexStart);
            if (indexStart + length > string.length()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid UTF-8 encoding");
            }
            return string.slice(indexStart, length);
        }

        int matchCount = 0;

        int previousIndex = 0;
        while (previousIndex < string.length()) {
            int matchIndex = string.indexOf(delimiter, previousIndex);
            // No match
            if (matchIndex < 0) {
                break;
            }
            // Reached the requested part?
            if (++matchCount == index) {
                return string.slice(previousIndex, matchIndex - previousIndex);
            }
            // Continue searching after the delimiter
            previousIndex = matchIndex + delimiter.length();
        }

        if (matchCount == index - 1) {
            // returns last section of the split
            return string.slice(previousIndex, string.length() - previousIndex);
        }

        // index is too big, null is returned
        return null;
    }

    @Description("removes whitespace from the beginning of a string")
    @ScalarFunction("ltrim")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice leftTrim(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return SliceUtf8.leftTrim(slice);
    }

    @Description("removes whitespace from the end of a string")
    @ScalarFunction("rtrim")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rightTrim(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return SliceUtf8.rightTrim(slice);
    }

    @Description("removes whitespace from the beginning and end of a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice trim(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return SliceUtf8.trim(slice);
    }

    @Description("converts the string to lower case")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice lower(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return toLowerCase(slice);
    }

    @Description("converts the string to upper case")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice upper(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return toUpperCase(slice);
    }

    @Description("transforms the string to normalized form")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice normalize(@SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.VARCHAR) Slice form)
    {
        Normalizer.Form targetForm;
        try {
            targetForm = Normalizer.Form.valueOf(form.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Normalization form must be one of [NFD, NFC, NFKD, NFKC]");
        }
        return utf8Slice(Normalizer.normalize(slice.toStringUtf8(), targetForm));
    }

    @Description("decodes the UTF-8 encoded string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return SliceUtf8.fixInvalidUtf8(slice);
    }

    @Description("decodes the UTF-8 encoded string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.VARCHAR) Slice replacementCharacter)
    {
        int count = countCodePoints(replacementCharacter);
        if (count > 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Replacement character string must empty or a single character");
        }

        OptionalInt replacementCodePoint;
        if (count == 1) {
            try {
                replacementCodePoint = OptionalInt.of(SliceUtf8.getCodePointAt(replacementCharacter, 0));
            }
            catch (InvalidUtf8Exception e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid replacement character");
            }
        }
        else {
            replacementCodePoint = OptionalInt.empty();
        }
        return SliceUtf8.fixInvalidUtf8(slice, replacementCodePoint);
    }

    @Description("decodes the UTF-8 encoded string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.BIGINT) long replacementCodePoint)
    {
        if (replacementCodePoint > MAX_CODE_POINT || Character.getType((int) replacementCodePoint) == SURROGATE) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid replacement character");
        }
        return SliceUtf8.fixInvalidUtf8(slice, OptionalInt.of((int) replacementCodePoint));
    }

    @Description("encodes the string to UTF-8")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toUtf8(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return slice;
    }
}
