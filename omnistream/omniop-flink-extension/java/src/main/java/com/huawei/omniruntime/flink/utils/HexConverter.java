package com.huawei.omniruntime.flink.utils;

/**
 * HexConverter
 *
 * @since 2025-04-27
 */
public class HexConverter {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /**
     * bytesToHex
     *
     * @param bytes bytes
     * @return String
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        hexString.append("Hex: ");
        char[] hex2 = new char[2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hex2[0] = HEX_ARRAY[v >>> 4];
            hex2[1] = HEX_ARRAY[v & 0x0F];
            hexString.append(hex2);
            hexString.append(" ");
        }

        return hexString.toString();
    }

    public static void main(String[] args) {
        byte[] byteArray = {(byte) 0x0A, (byte) 0x0F, (byte) 0xFF};
        String hexString = bytesToHex(byteArray);
        System.out.println(hexString); // Output: 0A0FFF
    }
}