package org.ecwid.ipcounter;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Класс для разбиения большого файла на части, их сортировки и обратной склейки.
 */

public class ExternalSort {
    private final Comparator<String> stringCpr = Comparator.naturalOrder();
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
        if (mbSize > 100 || mbSize < 0) mbSize = 100;
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
        ExecutorService executorService = Executors.newFixedThreadPool(15, Thread::new);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int currentPartSize = 0;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
                currentPartSize += line.length() + 1;
                if (currentPartSize >= maxPartSize) {
                    currentPartSize = 0;
                    ArrayList<String> threadList = new ArrayList<>(lines);
                    lines.clear();
                    executorService.execute(
                            () -> {
                                try {
                                    sortListAndSendToOutput(threadList, Thread.currentThread().getName());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
                }
            }
            sortListAndSendToOutput(lines, "final");
        }
        executorService.shutdown();
    }

    /**
     * сортирует список строк и отправляет в исходящий поток для записи.
     *
     * @param stringList список строк
     * @throws IOException ошибка ввода/вывода
     */
    private void sortListAndSendToOutput(List<String> stringList, String id) throws IOException {
        stringList.sort(stringCpr);
        String fileName = tempDir + "temp" + System.currentTimeMillis() + "_" + id;
        File file = new File(fileName);
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
        var ref = new Object() {
            private long counter = 0L;

            public long getCounter() {
                return counter;
            }

            public void setCounter(long counter) {
                this.counter = counter;
            }

            public void incCounter() {
                this.counter++;
            }
        };
        synchronized (ref) {
            Thread progressMonitor = new Thread(() -> {
                while (true) {
                    System.out.println("Strings processed = " + ref.getCounter());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            progressMonitor.setDaemon(true);
            progressMonitor.start();
        }
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
                writer.write(line.getString() + "\n");
                ref.incCounter();
                BufferedReader reader = map.remove(line);
                String nextLine = reader.readLine();
                if (nextLine != null) {
                    StringWrapper sw = new StringWrapper(nextLine);
                    map.put(sw, reader);
                    sorted.add(sw);
                }
            }
            System.out.println("written lines count = " + ref.getCounter());
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
     * Делегируем поведение компаратора в компаратор класса ExternalSort для того,
     * чтобы иметь возможность сортировать класс-оболочку StringWrapper.
     */
    private class ComparatorDelegate implements Comparator<StringWrapper> {
        @Override
        public int compare(StringWrapper o1, StringWrapper o2) {
            return stringCpr.compare(o1.string, o2.string);
        }
    }

    /**
     * Класс-оболочка для класса String. Необходим для дубликатов String, которые могут
     * конфликтовать в HashMap при слиянии файлов
     */
    private class StringWrapper implements Comparable<StringWrapper> {
        private final String string;

        public String getString() {
            return string;
        }

        public StringWrapper(String str) {
            this.string = str;
        }

        @Override
        public int compareTo(StringWrapper stringWrapper) {
            return string.compareTo(stringWrapper.string);
        }
    }

}

