package org.ecwid.ipcounter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class FileAnalyzer {
    private String filePath;

    public FileAnalyzer(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long bytesToMegabytes(long bytes) {
        return bytes / (1024L * 1024L);
    }

    public long bytesToKilobytes(long bytes) {
        return bytes / 1024L;
    }

    /**
     * генерирует случайное значние
     *
     * @param random генератор псевдослучайных чисел
     * @return число в диапазоне 0..255 включительно
     */
    private int getRandomInt(Random random) {
        return random == null ?
                new Random().nextInt(256) : random.nextInt(256);
    }

    /**
     * функция для создания тестового набора данных
     *
     * @param count    количество записей в создаваемом наборе данных
     * @param filePath путь к файлу набора данных
     * @throws IOException ошибка потоков ввода/вывода
     */
    public void createTestDataSet(long count, String filePath) throws IOException {
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

    /**
     * отображает в консоли прогресс выполнения задачи в процентах.
     *
     * @param progressPercentage процент для отображения в размере от 0 до 1
     */
    void updateProgress(double progressPercentage) {
        final int width = 50; // progress bar width in chars

        System.out.print("\r[");
        int i = 0;
        for (; i <= (int) (progressPercentage * width); i++) {
            System.out.print("\u2588");
        }
        for (; i < width; i++) {
            System.out.print(" ");
        }
        System.out.print(String.format("] %s", Math.round(progressPercentage * 100)) + "%");
    }


    public void countIpAddressesFromFile() throws IOException {
        long ipAddressCount =
                Files.lines(Paths.get(getFilePath()))
                        .parallel()
                        .flatMapToLong(line -> LongStream.of(ipHashCode(line)))
//                        .flatMap(line -> Arrays.stream(line.split("\\n")))
                        .distinct()
                        .count();

        System.out.println("ipAddressCount = " + ipAddressCount);
    }

    private long ipHashCode(String ipAddress) {
        long result = -1;
        if (addressIsValid(ipAddress)) {
            String[] parts = ipAddress.split("\\.");
            for (int j = 0; j < 4; j++) {
                result += Long.parseLong(parts[j]) * Math.pow(10, 3 * (3 - j));
            }
        }
        return result;
    }

    /**
     * Проверка строки ip-адреса на соответствие шаблону
     *
     * @param address строковое представление ip адреса
     * @return true - если соответствует шаблону, false - нет
     */
    private boolean addressIsValid(String address) {
        return address.matches("\\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.|$)){4}\\b");
    }

    /**
     * подсчитывает приблизительное количество уникальных ip-адресов в файле
     *
     * @throws IOException ошибка ввода/вывода
     */
    public void countApproxUniqueAddresses() throws IOException {
        /* массив для записи статистики использования
         * каждой из 4 частей адреса */
        int[] partOne = new int[256];
        int[] partTwo = new int[256];
        int[] partThree = new int[256];
        int[] partFour = new int[256];

        int[][] stats = new int[4][256];
        stats[0] = partOne;
        stats[1] = partTwo;
        stats[2] = partThree;
        stats[3] = partFour;

        long t;

        AtomicInteger stringsCount = new AtomicInteger();
        t = System.currentTimeMillis();
        Stream<String> lines = Files.lines(Paths.get(getFilePath()));
        lines.forEach((ipAddress) -> {
            String[] parts = ipAddress.split("\\.");
            for (int j = 0; j < 4; j++) {
                stats[j][Integer.parseInt(parts[j])] += 1;
            }
            stringsCount.getAndIncrement();
        });

        System.out.println("file reading ended " + (System.currentTimeMillis() - t) + "ms");

        t = System.currentTimeMillis();
        int uniques = 0;
        for (int i = 0; i < 256; i++) {
            int min = stats[0][i];
            for (int j = 1; j < 4; j++) {
                min = Math.min(min, stats[j][i]);
            }
            uniques += min;
        }
        System.out.println("counting ended " + (System.currentTimeMillis() - t) + "ms");

        System.out.println("Strings count = " + stringsCount);
        System.out.println("Unique addresses approx. = " + uniques);
    }

}


