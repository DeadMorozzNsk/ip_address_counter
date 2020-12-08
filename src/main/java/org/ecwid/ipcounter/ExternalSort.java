package org.ecwid.ipcounter;

import java.io.*;
import java.util.*;


/**
 * Класс для разбиения большого файла на части, их сортировки и обратной склейки.
 */

public class ExternalSort {
    private final Comparator<String> strCompr = Comparator.naturalOrder();
    private final String tempDir;
    private final List<File> outputFiles = new ArrayList<>();
    private int maxPartSize = 100 * 1024 * 1024;

    public ExternalSort(String tempDir) {
        this.tempDir = tempDir;
        File file = new File(tempDir);
        if (!file.exists() || !file.isDirectory())
            throw new IllegalArgumentException(
                    "Указанный путь не является директорией или не существует!");
    }

    /**
     * устанавливает размер частей для дробления.
     *
     * @param mbSize размер файла в <b>мегабайтах</b>
     */
    public void setMaximumPartSize(int mbSize) {
        this.maxPartSize = mbSize * 1024 * 1024;
    }

    /**
     * Читает входящий поток файла и делит на части согласно заданного размера.
     *
     * @param inputStream входящий поток
     * @throws IOException ошибка ввода/вывода
     */
    public void splitParts(InputStream inputStream) throws IOException {
        outputFiles.clear();
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int currentPartSize = 0;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
                currentPartSize += line.length() + 1;
                if (currentPartSize >= maxPartSize) {
                    currentPartSize = 0;
                    sortListAndSendToOutput(lines);
                }
            }
            sortListAndSendToOutput(lines);
        }
    }

    /**
     * сортирует список строк и отправляет в исходящий поток для записи.
     *
     * @param stringList список строк
     * @throws IOException ошибка ввода/вывода
     */
    private void sortListAndSendToOutput(List<String> stringList) throws IOException {
        stringList.sort(strCompr);
        File file = new File(tempDir + "temp" + System.currentTimeMillis());
        outputFiles.add(file);
        writeToFile(stringList, new FileOutputStream(file));
        stringList.clear();
    }

    /**
     * записывает список строк в исходящий поток. Каждый элемент списка записывается в новую строку.
     *
     * @param stringList список строк для записи
     * @param out        исходящий поток
     * @throws IOException ошибка ввода/вывода
     */
    private void writeToFile(List<String> stringList, OutputStream out) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
            for (String s : stringList) {
                writer.write(s + "\n");
            }
            writer.flush();
        }
    }

    /**
     * Читает временные файлы, созданные методом splitParts и соединяет их в один файл,
     * а временные файлы удаляет.
     *
     * @param out исходящий поток
     * @throws IOException ошибка ввода/вывода
     */
    public void mergeParts(OutputStream out) throws IOException {
        Map<StringWrapper, BufferedReader> map = new HashMap<>();
        List<BufferedReader> readers = new ArrayList<>();
        ComparatorDelegate delegate = new ComparatorDelegate();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
            for (int i = 0; i < outputFiles.size(); i++) {
                BufferedReader reader = new BufferedReader(new FileReader(outputFiles.get(i)));
                readers.add(reader);
                String line = reader.readLine();
                if (line != null) {
                    map.put(new StringWrapper(line), readers.get(i));
                }
            }
            List<StringWrapper> sorted = new LinkedList<>(map.keySet());
            while (map.size() > 0) {
                sorted.sort(delegate);
                StringWrapper line = sorted.remove(0);
                writer.write(line.string);
                writer.write("\n");
                BufferedReader reader = map.remove(line);
                String nextLine = reader.readLine();
                if (nextLine != null) {
                    StringWrapper sw = new StringWrapper(nextLine);
                    map.put(sw, reader);
                    sorted.add(sw);
                }
            }
        } finally {
            for (BufferedReader reader : readers) {
                try {
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (File output : outputFiles) {
                if (!output.delete())
                    System.err.println("Не удалось найти файл для удаления!!!");
            }
        }
    }


    /**
     * Делегируем поведение компаратора в компаратор класса ExternalSort.
     * Делегирование для того, чтобы иметь возможность сортировать класс-оболочку StringWrapper.
     */
    private class ComparatorDelegate implements Comparator<StringWrapper> {
        @Override
        public int compare(StringWrapper o1, StringWrapper o2) {
            return strCompr.compare(o1.string, o2.string);
        }
    }

    /**
     * Класс-оболочка для класса String. Необходим для дубликатов String, которые могут
     * конфликтовать в HashMap при слиянии файлов
     */
    private class StringWrapper implements Comparable<StringWrapper> {
        private final String string;

        public StringWrapper(String str) {
            this.string = str;
        }

        @Override
        public int compareTo(StringWrapper stringWrapper) {
            return string.compareTo(stringWrapper.string);
        }
    }

}

