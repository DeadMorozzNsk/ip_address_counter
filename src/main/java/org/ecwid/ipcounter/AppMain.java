package org.ecwid.ipcounter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;


public class AppMain {

    public static void main(String[] args) {
        String path = "src\\main\\java\\org\\ecwid\\ipcounter\\files\\testSet.txt";
        String tempDirPath = "src\\main\\java\\org\\ecwid\\ipcounter\\files\\temp\\";
        String targetFilePath = "src\\main\\java\\org\\ecwid\\ipcounter\\files\\temp\\result.txt";

        FileAnalyzer fileAnalyzer = new FileAnalyzer(path);
        long t = 0;
        try {
            File file = new File(path);
            if (!file.exists()) {
                fileAnalyzer.createTestDataSet(10000000, path);  /* create payload file if not exists */
            }
            t = System.currentTimeMillis();
            //fileAnalyzer.countIpAddressesFromFile();
            fileAnalyzer.countByExternalSort(tempDirPath, targetFilePath);
            //fileAnalyzer.countApproxUniqueAddresses();
            //naiveCountIpAddressesFromFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long msec = (System.currentTimeMillis() - t);
        long sec = msec / 1000;
        long mins = sec / 60;
        System.out.println("Time passed = " + mins + " min. " + (sec - mins * 60) + " sec.");
        Runtime runtime = Runtime.getRuntime();
        long memory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory in kilobytes: " + fileAnalyzer.bytesToKilobytes(memory));
        System.out.println("Used memory in megabytes: " + fileAnalyzer.bytesToMegabytes(memory));
    }

    /**
     * простейший путь подсчета количества уникальных строк.
     * использовался для проверки точности подсчета.
     *
     * @param path путь к файлу
     * @throws IOException ошибки при чтении/получению доступа к файлу
     */
    private static void naiveCountIpAddressesFromFile(String path) throws IOException {
        HashSet<String> al = new HashSet<>();
        Files.lines(Paths.get(path), StandardCharsets.UTF_8).forEach(al::add);
        System.out.println("unique addresses = " + al.size());
    }
}
