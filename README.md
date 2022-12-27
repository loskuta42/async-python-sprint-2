# Планировщик задач реализованный на потоках.

## Описание приложения

**1. Класс `Scheduler` ([scheduler.py](scheduler.py)).**

Функционал:
- Планировщик одновременно может выполнять до 10 задач (дефолтное значение, может быть изменено).
- Возможность добавить задачу в планировщик и запустить её в рамках ограничений планировщика и настроек, указанных в задаче.
- При штатном завершение работы планировщик сохраняет статус выполняемых и ожидающих задач.
- После рестарта восстанавливается последнее состояние и задачи продолжают выполняться.

**2. Класс `Job` ([job.py](job.py)).**

Функционал:
- У задачи может быть указана длительность выполнения (опциональный параметр). Если параметр указан, то задача прекращает выполняться, если время выполнения превысило указанный параметр.
- У задачи может быть указано время запуска (опциональный параметр). Если параметр указан, то задача стартует в указанный временный период.
- У задачи может быть указан параметр количества рестартов (опциональный параметр). Если в ходе выполнения задачи произошёл сбой или задачи-зависимости не были выполнены, то задача будет перезапущена указанное количество раз. Если параметр не указан, то количество рестартов равно 0.
- У задачи может быть указаны зависимости — задача или задачи, от которых зависит её выполнение (опциональный параметр). Если параметр указан, то задача не может стартовать до момента, пока не будут завершены задачи-зависимости.


**3. Планировщик протестирован на следующих задачах.**

- работа с файловой системой: создание, удаление, изменение директорий и файлов;
- работа с файлами: создание, чтение, запись;
- работа с сетью: обработка ссылок (GET-запросы) и анализ полученного результата;
- поддерживает конвейер выполнения основной задачи минимум из 3 задач, зависящих друг от друга и выполняющихся последовательно друг за другом.

## Особенности приложения:

1. Используются потоки, корутины и генераторы.
2. Используются только встроенные библиотеки и модули `Python`.
3. Используются концепции ООП.
4. Используется кастомный класс убиваемого потока. Идея взята из статьи - https://www.geeksforgeeks.org/python-different-ways-to-kill-a-thread/ .
