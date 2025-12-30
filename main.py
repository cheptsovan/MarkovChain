from markov_model import MarkovModel
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_postgresql_connection():
    #Проверка подключения к PostgreSQL
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv()

        db_params = {
            'dbname': os.getenv('DB_NAME', 'markov_chain'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }

        conn = psycopg2.connect(**db_params)
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        return False


def main():
    print("=" * 60)
    print("МОДЕЛЬ МАРКОВА ДЛЯ ТЕОРИИ ИНФОРМАЦИИ")
    print("=" * 60)

    # Проверяем PostgreSQL
    print("\n1. Проверка подключения к PostgreSQL...")
    if not check_postgresql_connection():
        print("\nОШИБКА: Не удалось подключиться к PostgreSQL!")
        print("Убедитесь, что:")
        print("1. PostgreSQL установлен и запущен")
        print("2. База данных 'markov_chain' создана")
        print("3. Файл .env с настройками существует")
        return

    print("✓ Подключение к PostgreSQL успешно")

    # Проверяем файл с текстом
    print("\n2. Поиск текстового файла...")

    # Возможные имена файлов
    possible_files = [
        "texts.txt",
        "data.txt",
        "corpus.txt",
        "input.txt",
        "текст.txt"
    ]

    text_file = None
    for file in possible_files:
        if os.path.exists(file):
            text_file = file
            break

    if not text_file:
        # Запрашиваем у пользователя
        text_file = input("Введите путь к текстовому файлу: ").strip()
        if not os.path.exists(text_file):
            print(f"Файл '{text_file}' не найден!")
            return

    print(f"✓ Найден файл: {text_file}")
    file_size = os.path.getsize(text_file) / (1024 * 1024)
    print(f"  Размер: {file_size:.2f} МБ")

    # Создаем и обучаем модель
    print("\n3. Обучение модели Маркова...")
    print("   (Это займет некоторое время, зависит от размера файла)")

    model = MarkovModel()

    try:
        # Обучаем модель с bulk-вставкой для скорости
        use_bulk = input("\nИспользовать быструю bulk-вставку? (y/n, рекомендуется y): ").lower() == 'y'

        model.train_from_file(
            text_file,
            max_order=13,
            use_bulk_insert=use_bulk
        )

        # Анализ результатов
        print("\n" + "=" * 60)
        print("ОБУЧЕНИЕ ЗАВЕРШЕНО!")
        print("=" * 60)

        model.analyze_contexts()

        # Примеры
        print("\nПримеры условных вероятностей:")
        test_contexts = [
            (' ', 1),
            ('я', 1),
            ('на', 2),
            ('что', 3),
            ('привет', 6)
        ]

        for context, order in test_contexts:
            if len(context) == order:
                probs = model.get_probabilities(context)
                if probs:
                    top5 = sorted(probs.items(), key=lambda x: x[1], reverse=True)[:3]
                    print(f"\nПосле '{context}' (порядок {order}):")
                    for char, prob in top5:
                        print(f"  '{char}': {prob:.4f}")

        # Генерация текста
        print("\n" + "=" * 60)
        generate = input("Сгенерировать пример текста? (y/n): ").lower() == 'y'
        if generate:
            seed = input("Введите начальную последовательность (например 'я '): ").strip() or 'я '
            length = int(input("Длина текста (символов): ").strip() or 100)

            print(f"\nГенерация текста из seed='{seed}':")
            print("-" * 40)
            generated = model.generate_text(seed, length)
            print(generated)
            print("-" * 40)

        print("\nДля анализа данных используйте:")
        print("1. pgAdmin или DBeaver для просмотра БД")
        print("2. SQL запросы из analyze_postgres.py")

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        model.close()
        print("\nРабота завершена!")


if __name__ == "__main__":
    main()