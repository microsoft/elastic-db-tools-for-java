package com.microsoft.azure.elasticdb.shard.utils;


import java.util.Locale;

public class StringUtilsLocal {
    /// <summary>
    /// Lookup array for converting byte[] to String.
    /// </summary>
    private static char[] byteToCharLookup = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    /// <summary>
    /// Creates a formatted String which is culture invariant with given arguments.
    /// </summary>
    /// <param name="input">Input String.</param>
    /// <param name="args">Collection of formatting arguments.</param>
    /// <returns>Formatted String.</returns>
    public static String formatInvariant(String input, Object... args)
    {
        return String.format(Locale.getDefault(), input, args);
    }

    /// <summary>
    /// Converts the given byte array to its String representation.
    /// </summary>
    /// <param name="input">Input byte array.</param>
    /// <returns>String representation of the byte array.</returns>
    public static String byteArrayToString(byte[] input)
    {
        assert input != null;

        StringBuilder result = new StringBuilder((input.length + 1) * 2).append("0x");

        for (int i = 0; i < input.length; i++) {
            byte b = input[i];
            result.append(byteToCharLookup[b >> 4]).append(byteToCharLookup[b & 0x0f]);
        }

        return result.toString();
    }

    /// <summary>
    /// Converts the given String to its byte array representation.
    /// </summary>
    /// <param name="input">Input String.</param>
    /// <returns>Byte representation of the String.</returns>
    public static byte[] stringToByteArray(String input)
    {
        assert input != null;

        byte[] result = new byte[(input.length() - 2) / 2];

        for (int i = 2, j = 0; i < input.length(); i += 2, j++)
        {
            result[j] = (byte)(charToByte(input.charAt(i)) * 16 + charToByte(input.charAt(i + 1)));
        }

        return result;
    }

    /// <summary>
    /// Converts given character to its binary representation.
    /// </summary>
    /// <param name="c">Input character.</param>
    /// <returns>Byte representation of input character.</returns>
    private static byte charToByte(char c)
    {
        switch (c)
        {
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
}
