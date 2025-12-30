import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()


def connect_db():
    #Подключение к PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME', 'markov_chain'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432')
    )
    return conn


def analyze_overall_frequencies(conn):
    #Анализ общих частот
    cursor = conn.cursor()

    print("ОБЩИЕ ЧАСТОТЫ СИМВОЛОВ")

    # Топ-20 символов
    cursor.execute('''
        SELECT symbol, frequency 
        FROM overall_frequencies 
        ORDER BY frequency DESC 
        LIMIT 20
    ''')

    results = cursor.fetchall()
    total = sum(freq for _, freq in results)

    print("\nТоп-20 символов:")
    print("-" * 40)
    print(f"{'Символ':<10} {'Частота':<12} {'Вероятность':<12}")
    print("-" * 40)

    for symbol, freq in results:
        prob = freq / total if total > 0 else 0
        symbol_display = repr(symbol).replace("'", "")
        print(f"{symbol_display:<10} {freq:<12,} {prob:.6f}")

    return results


def analyze_markov_orders(conn):
    #Анализ разных порядков модели
    cursor = conn.cursor()

    print("\nСРАВНЕНИЕ ПОРЯДКОВ МОДЕЛИ")

    orders = []
    context_counts = []
    total_frequencies = []

    for n in range(1, 14):
        # Количество уникальных контекстов
        cursor.execute(f'SELECT COUNT(DISTINCT context) FROM markov_{n}')
        contexts = cursor.fetchone()[0]

        # Общее количество записей
        cursor.execute(f'SELECT SUM(frequency) FROM markov_{n}')
        total_freq = cursor.fetchone()[0] or 0

        orders.append(n)
        context_counts.append(contexts)
        total_frequencies.append(total_freq)

        print(f"Порядок {n:2d}: {contexts:>10,} контекстов, {total_freq:>15,} вхождений")

    # График 1: Количество контекстов
    plt.figure(figsize=(12, 5))

    plt.subplot(1, 2, 1)
    plt.plot(orders, context_counts, 'bo-', linewidth=2, markersize=8)
    plt.xlabel('Порядок модели (n)')
    plt.ylabel('Количество уникальных контекстов')
    plt.title('Рост числа контекстов с увеличением порядка')
    plt.grid(True, alpha=0.3)
    plt.yscale('log')

    # График 2: Общее количество вхождений
    plt.subplot(1, 2, 2)
    plt.plot(orders, total_frequencies, 'ro-', linewidth=2, markersize=8)
    plt.xlabel('Порядок модели (n)')
    plt.ylabel('Общее количество вхождений')
    plt.title('Суммарная частота всех n-грамм')
    plt.grid(True, alpha=0.3)
    plt.yscale('log')

    plt.tight_layout()
    plt.savefig('markov_analysis.png', dpi=150, bbox_inches='tight')
    print(f"\nГрафики сохранены в 'markov_analysis.png'")

    return orders, context_counts, total_frequencies


def analyze_specific_contexts(conn):
    #Анализ конкретных контекстов
    cursor = conn.cursor()

    print("\nПРИМЕРЫ УСЛОВНЫХ ВЕРОЯТНОСТЕЙ")

    test_cases = [
        (1, ' ', 'пробел'),
        (1, 'я', '"я"'),
        (2, 'на', '"на"'),
        (3, 'что', '"что"'),
        (4, 'котор', '"котор"'),
        (5, 'которо', '"которо"')
    ]

    for n, context, description in test_cases:
        cursor.execute(f'''
            SELECT symbol, frequency 
            FROM markov_{n} 
            WHERE context = %s 
            ORDER BY frequency DESC 
            LIMIT 5
        ''', (context,))

        results = cursor.fetchall()

        if results:
            total = sum(freq for _, freq in results)
            print(f"\nПосле {description} (порядок {n}):")
            for symbol, freq in results:
                prob = freq / total if total > 0 else 0
                symbol_display = repr(symbol).replace("'", "")
                print(f"  {symbol_display}: {prob:.4f} ({freq:,})")
        else:
            print(f"\nПосле {description}: нет данных")


def calculate_entropy(conn, n):
    #Расчет энтропии для порядка n
    cursor = conn.cursor()

    cursor.execute(f'''
        SELECT context, SUM(frequency) as total
        FROM markov_{n}
        GROUP BY context
        HAVING SUM(frequency) > 100
        LIMIT 1000
    ''')

    contexts = cursor.fetchall()

    total_entropy = 0
    count = 0

    for context, total in contexts:
        cursor.execute(f'''
            SELECT frequency FROM markov_{n} WHERE context = %s
        ''', (context,))

        frequencies = [row[0] for row in cursor.fetchall()]

        # Расчет энтропии для данного контекста
        entropy = 0
        for freq in frequencies:
            p = freq / total
            if p > 0:
                entropy -= p * np.log2(p)

        total_entropy += entropy
        count += 1

    return total_entropy / count if count > 0 else 0


def main():
    print("АНАЛИЗ МОДЕЛИ МАРКОВА В POSTGRESQL")
    print("=" * 50)

    try:
        conn = connect_db()
        print("Подключение к PostgreSQL установлено\n")

        # 1. Общие частоты
        analyze_overall_frequencies(conn)

        # 2. Анализ порядков
        orders, contexts, frequencies = analyze_markov_orders(conn)

        # 3. Конкретные примеры
        analyze_specific_contexts(conn)

        # 4. Энтропия (опционально)
        print("\nРАСЧЕТ ЭНТРОПИИ")
        for n in [1, 2, 3, 5, 7, 10]:
            entropy = calculate_entropy(conn, n)
            print(f"Средняя энтропия для порядка {n}: {entropy:.4f} бит")

        conn.close()

        print("\n" + "=" * 50)
        print("Анализ завершен!")
        print("\nДля дополнительного анализа используйте SQL запросы:")
        print("""
-- Примеры SQL запросов:
SELECT * FROM overall_frequencies ORDER BY frequency DESC LIMIT 10;

SELECT context, symbol, frequency 
FROM markov_3 
WHERE context = 'при' 
ORDER BY frequency DESC 
LIMIT 5;

-- Количество контекстов каждого порядка
SELECT 'markov_1' as table_name, COUNT(*) FROM markov_1
UNION ALL SELECT 'markov_2', COUNT(*) FROM markov_2
-- ... и так далее для всех порядков
        """)

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()