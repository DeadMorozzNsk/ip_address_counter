package org.ecwid.ipcounter;

import java.io.*;
import java.util.*;

public final class FileAnalyzer {
    private static final long MEGABYTE = 1024L * 1024L;
    private static final long KILOBYTE = 1024L;

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static long bytesToKilobytes(long bytes) {
        return bytes / KILOBYTE;
    }

    private static int getRandomInt(Random random) {
        return random == null ?
                new Random().nextInt(255) : random.nextInt(255);
    }

    public static void createTestDataSet(long count, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            String msg = "Creating file... \n";
            System.out.println(
                    file.createNewFile() ? (msg += "success!") : (msg += "failed!")
            );
        }
        try (FileOutputStream out = new FileOutputStream(filePath)) {
            Random random = new Random();
            for (long i = 0L; i < count; i++) {
                String ipAddress = "";
                for (int j = 0; j < 4; j++) {
                    ipAddress = j == 3 ? (ipAddress + getRandomInt(random) + "\n")
                            : (ipAddress + getRandomInt(random) + ".");
                }
                char[] ch = ipAddress.toCharArray();
                byte[] arr = new byte[ch.length];
                for (int k = 0; k < ch.length; k++) {
                    arr[k] = (byte) ch[k];
                }
                out.write(arr);
            }
        }

    }

    public static void countIpAddressesFromFile(String filePath) throws IOException {
        /* ИТАК, хитрый план:
        * читаем байтами по 50-100 мб
        * символ \n = 10  для  сплита */

        try (FileInputStream in = new FileInputStream(filePath)) {
            byte[] arr = new byte[50*1024*1024];
            int x;
           List<String> ipAddresses = new ArrayList<>();
            while ((x = in.read(arr)) != -1) {
                ipAddresses.add(new String(arr, 0, x));
            }
            System.out.println("unique addresses = " + ipAddresses.size());
        }
    }
}
