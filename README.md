Задача:
===
Дан простой текстовый файл с IPv4 адресами. Одна строка – один адрес, примерно так:

```
145.67.23.4
8.34.5.23
89.54.3.124
89.54.3.124
3.45.71.5
...
```

Файл в размере не ограничен и может занимать десятки и сотни гигабайт.
Необходимо посчитать количество __уникальных адресов__ в этом файле, затратив как можно меньше памяти и времени. Существует "наивный" алгоритм решения данной задачи (читаем строка за строкой, кладем строки в HashSet), желательно чтобы ваша реализация была лучше этого простого, наивного алгоритма.

Немного деталей:
---
+ Использовать можно только возможности стандартной библиотеки Java/Kotlin
+ Писать нужно на Java (версия 11 и выше) или Kotlin.
+ В задании должен быть рабочий метод main(), это должно быть готовое приложение, а не просто библиотека
+ Сделанное задание необходимо разместить на GitHub

---
Прежде чем отправить задание, имеет смысл проверить его вот на этом [файле](https://ecwid-vgv-storage.s3.eu-central-1.amazonaws.com/ip_addresses.zip). Внимание – файл весит около 20Gb, а распаковывается приблизительно в 120Gb.