package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.Locale;

/**
 * Utility methods for string manipulation.
 */
public final class StringUtilsLocal {
    /**
     * Lookup array for converting byte[] to string.
     */
    private static char[] byteToCharLookup = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * Creates a formatted String which is culture invariant with given arguments.
     *
     * @param input Input String.
     * @param args  Collection of formatting arguments.
     * @return Formatted String.
     */
    public static String FormatInvariant(String input, Object... args) {
        return String.format(Locale.getDefault(), input, args);
    }

    /**
     * Converts the given byte array to its string representation.
     *
     * @param input Input byte array.
     * @return String representation of the byte array.
     */
    public static String byteArrayToString(byte[] input) {
        assert input != null;

        StringBuilder result = new StringBuilder((input.length + 1) * 2).append("0x");

        for (int i = 0; i < input.length; i++) {
            byte b = input[i];
            result.append(byteToCharLookup[b >> 4]).append(byteToCharLookup[b & 0x0f]);
        }

        return result.toString();
    }

    /**
     * Converts the given string to its byte array representation.
     *
     * @param input Input string.
     * @return Byte representation of the string.
     */
    public static byte[] stringToByteArray(String input) {
        assert input != null;

        byte[] result = new byte[(input.length() - 2) / 2];

        for (int i = 2, j = 0; i < input.length(); i += 2, j++) {
            result[j] = (byte) (charToByte(input.charAt(i)) * 16 + charToByte(input.charAt(i + 1)));
        }

        return result;
    }

    /**
     * Converts given character to its binary representation.
     *
     * @param c Input character.
     * @return Byte representation of input character.
     */
    private static byte charToByte(char c) {
        switch (c) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'a':
            case 'A':
                return 0xa;
            case 'b':
            case 'B':
                return 0xb;
            case 'c':
            case 'C':
                return 0xc;
            case 'd':
            case 'D':
                return 0xd;
            case 'e':
            case 'E':
                return 0xe;
            case 'f':
            case 'F':
                return 0xf;
            default:
                throw new IllegalStateException("Unexpected byte value.");
        }
    }

    /**
     * This method replaces the .NET string method 'Substring' when 'start' is a method
     * call or calculated value to ensure that 'start' is obtained just once.
     */
    public static String substring(String string, int start, int length) {
        if (length < 0)
            throw new IndexOutOfBoundsException("Parameter length cannot be negative.");

        return string.substring(start, start + length);
    }

    /**
     * This method replaces the .NET static string method 'IsNullOrEmpty'.
     */
    public static boolean isNullOrEmpty(String string) {
        return string == null || string.length() == 0;
    }

    /**
     * This method replaces the .NET static string method 'IsNullOrWhiteSpace'.
     */
    public static boolean isNullOrWhiteSpace(String string) {
        if (string == null)
            return true;

        for (int index = 0; index < string.length(); index++) {
            if (!Character.isWhitespace(string.charAt(index)))
                return false;
        }

        return true;
    }

    /**
     * This method replaces the .NET static string method 'Join' (2 parameter version).
     */
    public static String join(String separator, String[] stringArray) {
        if (stringArray == null)
            return null;
        else
            return join(separator, stringArray, 0, stringArray.length);
    }

    /**
     * This method replaces the .NET static string method 'Join' (4 parameter version).
     */
    public static String join(String separator, String[] stringArray, int startIndex, int count) {
        String result = "";

        if (stringArray == null)
            return null;

        for (int index = startIndex; index < stringArray.length && index - startIndex < count; index++) {
            if (separator != null && index > startIndex)
                result += separator;

            if (stringArray[index] != null)
                result += stringArray[index];
        }

        return result;
    }

    /**
     * This method replaces the .NET string method 'Remove' (1 parameter version).
     */
    public static String remove(String string, int start) {
        return string.substring(0, start);
    }

    /**
     * This method replaces the .NET string method 'Remove' (2 parameter version).
     */
    public static String remove(String string, int start, int count) {
        return string.substring(0, start) + string.substring(start + count);
    }

    /**
     * This method replaces the .NET string method 'TrimEnd'.
     */
    public static String trimEnd(String string, Character... charsToTrim) {
        if (string == null || charsToTrim == null)
            return string;

        int lengthToKeep = string.length();
        for (int index = string.length() - 1; index >= 0; index--) {
            boolean removeChar = false;
            if (charsToTrim.length == 0) {
                if (Character.isWhitespace(string.charAt(index))) {
                    lengthToKeep = index;
                    removeChar = true;
                }
            } else {
                for (int trimCharIndex = 0; trimCharIndex < charsToTrim.length; trimCharIndex++) {
                    if (string.charAt(index) == charsToTrim[trimCharIndex]) {
                        lengthToKeep = index;
                        removeChar = true;
                        break;
                    }
                }
            }
            if (!removeChar)
                break;
        }
        return string.substring(0, lengthToKeep);
    }

    /**
     * This method replaces the .NET string method 'TrimStart'.
     */
    public static String trimStart(String string, Character... charsToTrim) {
        if (string == null || charsToTrim == null)
            return string;

        int startingIndex = 0;
        for (int index = 0; index < string.length(); index++) {
            boolean removeChar = false;
            if (charsToTrim.length == 0) {
                if (Character.isWhitespace(string.charAt(index))) {
                    startingIndex = index + 1;
                    removeChar = true;
                }
            } else {
                for (int trimCharIndex = 0; trimCharIndex < charsToTrim.length; trimCharIndex++) {
                    if (string.charAt(index) == charsToTrim[trimCharIndex]) {
                        startingIndex = index + 1;
                        removeChar = true;
                        break;
                    }
                }
            }
            if (!removeChar)
                break;
        }
        return string.substring(startingIndex);
    }

    /**
     * This method replaces the .NET string method 'Trim' when arguments are used.
     */
    public static String trim(String string, Character... charsToTrim) {
        return trimEnd(trimStart(string, charsToTrim), charsToTrim);
    }

    /**
     * This method is used for string equality comparisons when the option
     * 'Use helper 'stringsEqual' method to handle null strings' is selected
     * (The Java String 'equals' method can't be called on a null instance).
     */
    public static boolean stringsEqual(String s1, String s2) {
        if (s1 == null && s2 == null)
            return true;
        else
            return s1 != null && s1.equals(s2);
    }

    /**
     * This method replaces the .NET string method 'PadRight' (1 parameter version).
     */
    public static String padRight(String string, int totalWidth) {
        return padRight(string, totalWidth, ' ');
    }

    /**
     * This method replaces the .NET string method 'PadRight' (2 parameter version).
     */
    public static String padRight(String string, int totalWidth, char paddingChar) {
        StringBuilder sb = new StringBuilder(string);

        while (sb.length() < totalWidth) {
            sb.append(paddingChar);
        }

        return sb.toString();
    }

    /**
     * This method replaces the .NET string method 'PadLeft' (1 parameter version).
     */
    public static String padLeft(String string, int totalWidth) {
        return padLeft(string, totalWidth, ' ');
    }

    /**
     * This method replaces the .NET string method 'PadLeft' (2 parameter version).
     */
    public static String padLeft(String string, int totalWidth, char paddingChar) {
        StringBuilder sb = new StringBuilder("");

        while (sb.length() + string.length() < totalWidth) {
            sb.append(paddingChar);
        }

        sb.append(string);
        return sb.toString();
    }

    /**
     * This method replaces the .NET string method 'LastIndexOf' (string version).
     */
    public static int lastIndexOf(String string, String value, int startIndex, int count) {
        int leftMost = startIndex + 1 - count;
        int rightMost = startIndex + 1;
        String substring = string.substring(leftMost, rightMost);
        int lastIndexInSubstring = substring.lastIndexOf(value);
        if (lastIndexInSubstring < 0)
            return -1;
        else
            return lastIndexInSubstring + leftMost;
    }

    /**
     * This method replaces the .NET string method 'IndexOfAny' (1 parameter version).
     */
    public static int indexOfAny(String string, char[] anyOf) {
        int lowestIndex = -1;
        for (char c : anyOf) {
            int index = string.indexOf(c);
            if (index > -1) {
                if (lowestIndex == -1 || index < lowestIndex) {
                    lowestIndex = index;

                    if (index == 0)
                        break;
                }
            }
        }

        return lowestIndex;
    }

    /**
     * This method replaces the .NET string method 'IndexOfAny' (2 parameter version).
     */
    public static int indexOfAny(String string, char[] anyOf, int startIndex) {
        int indexInSubstring = indexOfAny(string.substring(startIndex), anyOf);
        if (indexInSubstring == -1)
            return -1;
        else
            return indexInSubstring + startIndex;
    }

    /**
     * This method replaces the .NET string method 'IndexOfAny' (3 parameter version).
     */
    public static int indexOfAny(String string, char[] anyOf, int startIndex, int count) {
        int endIndex = startIndex + count;
        int indexInSubstring = indexOfAny(string.substring(startIndex, endIndex), anyOf);
        if (indexInSubstring == -1)
            return -1;
        else
            return indexInSubstring + startIndex;
    }

    /**
     * This method replaces the .NET string method 'LastIndexOfAny' (1 parameter version).
     */
    public static int lastIndexOfAny(String string, char[] anyOf) {
        int highestIndex = -1;
        for (char c : anyOf) {
            int index = string.lastIndexOf(c);
            if (index > highestIndex) {
                highestIndex = index;

                if (index == string.length() - 1)
                    break;
            }
        }

        return highestIndex;
    }

    /**
     * This method replaces the .NET string method 'LastIndexOfAny' (2 parameter version).
     */
    public static int lastIndexOfAny(String string, char[] anyOf, int startIndex) {
        String substring = string.substring(0, startIndex + 1);
        int lastIndexInSubstring = lastIndexOfAny(substring, anyOf);
        if (lastIndexInSubstring < 0)
            return -1;
        else
            return lastIndexInSubstring;
    }

    /**
     * This method replaces the .NET string method 'LastIndexOfAny' (3 parameter version).
     */
    public static int lastIndexOfAny(String string, char[] anyOf, int startIndex, int count) {
        int leftMost = startIndex + 1 - count;
        int rightMost = startIndex + 1;
        String substring = string.substring(leftMost, rightMost);
        int lastIndexInSubstring = lastIndexOfAny(substring, anyOf);
        if (lastIndexInSubstring < 0)
            return -1;
        else
            return lastIndexInSubstring + leftMost;
    }
}
