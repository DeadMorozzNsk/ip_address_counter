package org.ecwid.ipcounter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;


//    private static int convertByteArrayToIntValue(byte[] arr, int lastIndex) {
//        int result = 0;
//        int iter = 0;
//        for (int i = lastIndex; i >= 0; i--) {
//            result = (int) Math.pow(i, lastIndex);
//        }
//        if (lastIndex == 0) return arr[lastIndex];
//        else if (lastIndex == 1) {
//            result = arr[0] * 10 + arr[1];
//        } else if (lastIndex == 2) {
//            result = arr[0] * 100 + arr[1] * 10 + arr[2];
//        }
//        return result;
//    }
//}
public final class FileAnalyzer {
    private static final long MEGABYTE = 1024L * 1024L;
    private static final long KILOBYTE = 1024L;

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static long bytesToKilobytes(long bytes) {
        return bytes / KILOBYTE;
    }

    /**
     * генерирует случайное значние
     * @param random генератор псевдослучайных чисел
     * @return число в диапазоне 0..255 включительно
     */
    private static int getRandomInt(Random random) {
        return random == null ?
                new Random().nextInt(256) : random.nextInt(256);
    }

    /**
     * функция для создания тестового набора данных
     * @param count количество записей в создаваемом наборе данных
     * @param filePath путь к файлу набора данных
     * @throws IOException ошибка потоков ввода/вывода
     */
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

    /**
     * подсчитывает приблизительное количтество уникальных ip-адресов в файле
     * @param filePath путь к файлу
     * @throws IOException ошибка ввода/вывода
     */
    public static void countIpAddressesFromFile(String filePath) throws IOException {
        /* ИТАК, хитрый план:
        * читаем байтами по 50-100 мб
        * байт символа \n = 10  для  сплита */

        int rearingBlockSize = 50; /* размер считываемого блока в мегабайтах */

        /* массив для записи статистики использования
        * каждой из 4 честей адреса */
        int[] partOne = new int[256];
        int[] partTwo = new int[256];
        int[] partThree = new int[256];
        int[] partFour = new int[256];

        int[][] stats = new int[4][256];
        stats[0] = partOne;
        stats[1] = partTwo;
        stats[2] = partThree;
        stats[3] = partFour;

        try (FileInputStream in = new FileInputStream(filePath)) {
            byte[] arr = new byte[rearingBlockSize * 1024 * 1024];
            byte[] tmp = new byte[16];
            int tmpIndex = 0;
            int x;
            int stringsCount = 0;
            int validStringsCount = 0;
            while ((x = in.read(arr)) != -1) {
                for (int i = 0; i < x; i++) {
                    /*проходим по массиву байт  заданной длины
                    * и заполняем временный массив до знака переноса строки
                    * затем считываем строку, валидируем как ip-адрес
                    * затем инкрементируем соответствующие массивы
                    * в структуре сбора статистики использования */
                    if (arr[i] != (byte) 10) {
                        tmp[tmpIndex] = arr[i];
                        tmpIndex++;
                    } else {
                        stringsCount++;
                        String ipAddress = new String(tmp, 0, tmpIndex);
                        tmpIndex = 0;
                        if (addressIsValid(ipAddress)) {
                            String[] parts = ipAddress.split("\\.");
                            for (int j = 0; j < 4; j++) {
                                stats[j][Integer.parseInt(parts[j])] += 1;
                            }
                            validStringsCount++;
                        }
                    }
                }
            }
            int uniques = 0;
            for (int i = 0; i < 256; i++) {
                int min = stats[0][i];
                for (int j = 1; j < 4; j++) {
                    min = Math.min(min, stats[j][i]);
                }
                uniques+= min;
            }

            System.out.println("strings count = " + stringsCount);
            System.out.println("Valid strings count = " + validStringsCount);
            System.out.println("Unique addresses approx. = " + uniques);
        }
    }

    /**
     * Проверка строки ip-адреса на соответствие шаблону
     * @param address строковое представление ip адреса
     * @return true - если соответствует шаблону, false - нет
     */
    private static boolean addressIsValid(String address) {
        return address.matches("\\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.|$)){4}\\b");
    }
}


