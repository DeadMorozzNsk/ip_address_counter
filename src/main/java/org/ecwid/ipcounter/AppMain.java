package org.ecwid.ipcounter;

import java.io.File;
import java.io.IOException;

import static org.ecwid.ipcounter.FileAnalyzer.*;

public class AppMain {
    public static void main(String[] args) {
        String path = "src\\main\\java\\org\\ecwid\\ipcounter\\files\\testSet.txt";

        long t = System.currentTimeMillis(); /* start time measuring */
        try {
            File file = new File(path);
            if (!file.exists()) {
                createTestDataSet(10000000, path); /* create payload file if not exists */
            }
            countIpAddressesFromFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* stop the timer */
        System.out.println("Time passed = " + (System.currentTimeMillis() - t) + " ms");
        Runtime runtime = Runtime.getRuntime();
//        runtime.gc();
        long memory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory in bytes: " + memory);
        System.out.println("Used memory in kilobytes: " + bytesToKilobytes(memory));
        System.out.println("Used memory in megabytes: " + bytesToMegabytes(memory));
    }
}
