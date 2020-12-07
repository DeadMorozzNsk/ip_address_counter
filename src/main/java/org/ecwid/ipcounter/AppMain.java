package org.ecwid.ipcounter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;

import static org.ecwid.ipcounter.FileAnalyzer.*;


public class AppMain {

    static void updateProgress(double progressPercentage) {
        final int width = 50; // progress bar width in chars

        System.out.print("\r[");
        int i = 0;
        for (; i <= (int) (progressPercentage * width); i++) {
            System.out.print("\u2588");
        }
        for (; i < width; i++) {
            System.out.print(" ");
        }
        System.out.print("]");
    }

    public static void main(String[] args) {
        /*try {
            for (double progressPercentage = 0.0; progressPercentage < 1.0; progressPercentage += 0.01) {
                updateProgress(progressPercentage);
                Thread.sleep(20);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        String path = "src\\main\\java\\org\\ecwid\\ipcounter\\files\\testSet.txt";

        long t = 0;
        try {
            File file = new File(path);
            if (!file.exists()) {
                createTestDataSet(10000000, path);  /* create payload file if not exists */
            }
            t = System.currentTimeMillis();  /* start time measuring */
            countIpAddressesFromFile(path);
            //naiveCountIpAddressesFromFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* stop the timer */
        long msec = (System.currentTimeMillis() - t);
        long sec = msec/1000;
        long mins = sec/60;
        System.out.println("Time passed = " + msec + " ms");
        System.out.println("Time passed = " + mins );
        Runtime runtime = Runtime.getRuntime();
        long memory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory in kilobytes: " + bytesToKilobytes(memory));
        System.out.println("Used memory in megabytes: " + bytesToMegabytes(memory));
    }

    /**
     * простейший путь подсчета количества уникальных строк
     * @param path путь к файлу
     * @throws IOException ошибки при чтении/получению доступа к файлу
     */
    private static void naiveCountIpAddressesFromFile(String path) throws IOException {
        HashSet<String> al = new HashSet<>();
        Files.lines(Paths.get(path), StandardCharsets.UTF_8).forEach(al::add);
        System.out.println("unique addresses = " + al.size());
    }
}
