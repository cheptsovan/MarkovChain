from text_processor import TextProcessor
from database import MarkovDatabase
import time
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MarkovModel:
    def __init__(self):
        self.db = MarkovDatabase()
        self.processor = TextProcessor()

    def train_from_file(self, file_path, max_order=13, use_bulk_insert=False):
        """
        Обучение модели на текстовом файле

        Args:
            file_path: путь к текстовому файлу
            max_order: максимальный порядок модели Маркова (1-13)
            use_bulk_insert: использовать быструю bulk-вставку (требует больше памяти)
        """
        logger.info(f"Чтение файла {file_path}...")

        # Чтение файла с обработкой кодировки
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='cp1251') as f:
                    text = f.read()
            except:
                with open(file_path, 'r', encoding='latin-1') as f:
                    text = f.read()

        logger.info(f"Размер текста: {len(text):,} символов")

        # Нормализация текста
        logger.info("Нормализация текста...")
        start_time = time.time()
        normalized_text = self.processor.normalize_text(text)
        logger.info(f"После нормализации: {len(normalized_text):,} символов, время: {time.time() - start_time:.2f} сек")

        # Освобождаем память
        del text

        # Подключение к базе данных
        self.db.connect()
        self.db.create_tables()

        # Подсчет общих частот
        logger.info("Подсчет общих частот символов...")
        start_time = time.time()
        overall_freq = self.processor.count_overall_frequencies(normalized_text)
        self.db.save_overall_frequencies(overall_freq)
        logger.info(f"Общие частоты сохранены, время: {time.time() - start_time:.2f} сек")

        # Подсчет условных частот для разных порядков
        for n in range(1, max_order + 1):
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Подсчет частот для порядка {n}...")
            start_time = time.time()

            # Подсчет частот
            freq_start = time.time()
            markov_freq = self.processor.count_markov_frequencies(normalized_text, n)
            calc_time = time.time() - freq_start
            logger.info(f"  Подсчет завершен: {len(markov_freq):,} контекстов, время: {calc_time:.2f} сек")

            # Сохранение в БД
            save_start = time.time()
            if use_bulk_insert and n <= 5:  # Для высоких порядков bulk может быть неэффективен
                self.db.save_markov_frequencies_bulk(n, markov_freq)
            else:
                self.db.save_markov_frequencies(n, markov_freq)
            save_time = time.time() - save_start

            total_time = time.time() - start_time
            logger.info(f"  Сохранено в БД: время {save_time:.2f} сек, всего: {total_time:.2f} сек")

            # Очистка памяти
            del markov_freq

        # Вывод статистики
        logger.info(f"\n{'=' * 50}")
        logger.info("Обучение завершено!")
        stats = self.db.get_database_stats()

        logger.info("\nСтатистика базы данных:")
        logger.info(f"Размер БД: {stats.get('db_size', 'N/A')}")
        logger.info(f"Общих частот: {stats.get('overall_count', 'N/A'):,}")
        for n in range(1, max_order + 1):
            count = stats.get(f'markov_{n}_count', 0)
            logger.info(f"  Порядок {n}: {count:,} записей")

    def get_probabilities(self, context):
        """
        Получение вероятностей следующего символа для заданного контекста

        Args:
            context: строка с предыдущими символами (длина от 1 до 13)

        Returns:
            dict: словарь вероятностей символов
        """
        n = len(context)
        if n == 0 or n > 13:
            return {}

        return self.db.get_markov_probabilities(n, context)

    def generate_text(self, seed, length=100):
        """
        Генерация текста на основе обученной модели

        Args:
            seed: начальная последовательность символов
            length: длина генерируемого текста

        Returns:
            str: сгенерированный текст
        """
        if not self.db.conn:
            self.db.connect()

        result = seed
        current_context = seed[-min(len(seed), 13):]

        for i in range(length):
            # Получаем вероятности для текущего контекста
            probs = self.get_probabilities(current_context)

            # Если для данного контекста нет данных, уменьшаем длину контекста
            while not probs and len(current_context) > 1:
                current_context = current_context[1:]
                probs = self.get_probabilities(current_context)

            # Если совсем нет данных, используем общие вероятности
            if not probs:
                probs = self.db.get_overall_probabilities()

            # Выбираем следующий символ на основе вероятностей
            import random
            if probs:
                symbols, weights = zip(*probs.items())
                next_char = random.choices(symbols, weights=weights, k=1)[0]
            else:
                next_char = ' '

            result += next_char
            current_context = (current_context + next_char)[-13:]

            # Прогресс
            if (i + 1) % 50 == 0:
                logger.info(f"Сгенерировано {i + 1}/{length} символов")

        return result

    def analyze_contexts(self):
        """Анализ количества контекстов разных порядков"""
        if not self.db.conn:
            self.db.connect()

        stats = self.db.get_database_stats()

        print("\nАнализ модели Маркова:")
        print("Порядок | Уникальных контекстов")
        print("-" * 30)

        for n in range(1, 14):
            count = stats.get(f'markov_{n}_count', 0)
            print(f"{n:7d} | {count:20,}")

    def close(self):
        """Закрытие соединения с базой данных"""
        self.db.close()