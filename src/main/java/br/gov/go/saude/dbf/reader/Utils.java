package br.gov.go.saude.dbf.reader;

public final class Utils {
    public static int calcLengthTrimmingSpaces(byte[] arr, int start, int length) {
        int end = start + length - 1;

        while (end >= start && (arr[end] == ' ' || arr[end] == 0))
            end--;

        return end - start + 1;
    }


    public static int readLittleEndian(byte[] data, int offset) {
        int result = 0;

        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8)
            result |= (data[offset++] & 0xff) << shiftBy;

        return result;
    }

    public static boolean contains(byte[] data, int start, int end, byte value) {
        for (int i = start; i < end; i++)
            if (data[i] == value)
                return true;


        return false;
    }

    public static int parsePositiveInt(byte[] data, int start, int end) {
        int result = 0;

        for (int i = start; i < end && i < data.length; i++) {
            if (data[i] == ' ')
                break;

            if (data[i] < '0' || data[i] > '9')
                throw new NumberFormatException(String.format("Unexpected char '%c' in integer value", data[i]));

            result *= 10;
            result += (data[i] - (byte) '0');
        }

        return result;
    }

}
