import re
from collections import defaultdict


class TextProcessor:
    # Разрешенные символы
    ALLOWED_CHARS = set('абвгдеёжзийклмнопрстуфхцчшщъыьэюя ,.!?')

    @staticmethod
    def normalize_text(text):
        """Нормализация текста: приведение к нижнему регистру и фильтрация символов"""
        # Приводим к нижнему регистру
        text = text.lower()

        # Заменяем все символы, кроме разрешенных, на пробелы
        # и объединяем несколько пробелов в один
        text = ''.join(c if c in TextProcessor.ALLOWED_CHARS else ' ' for c in text)
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    @staticmethod
    def count_overall_frequencies(text):
        """Подсчет общих частот символов"""
        frequencies = defaultdict(int)
        for char in text:
            if char in TextProcessor.ALLOWED_CHARS:
                frequencies[char] += 1
        return dict(frequencies)

    @staticmethod
    def count_markov_frequencies(text, n):
        """
        Подсчет частот для модели Маркова порядка n

        Args:
            text: нормализованный текст
            n: количество предыдущих символов (от 1 до 13)

        Returns:
            dict: словарь, где ключ - контекст (n символов),
                  значение - словарь частот следующих символов
        """
        frequencies = defaultdict(lambda: defaultdict(int))

        # Проходим по тексту, рассматривая все последовательности длины n+1
        for i in range(len(text) - n):
            context = text[i:i + n]
            next_char = text[i + n]

            # Учитываем только если оба символа разрешены
            if all(c in TextProcessor.ALLOWED_CHARS for c in context) and next_char in TextProcessor.ALLOWED_CHARS:
                frequencies[context][next_char] += 1

        return dict(frequencies)