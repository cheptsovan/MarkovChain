import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class MarkovDatabase:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'markov_chain'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }

    def connect(self):
        #Создание подключения к базе данных PostgreSQL
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            logger.info("Подключение к PostgreSQL установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            raise

    def create_tables(self):
        #Создание таблиц для хранения частот
        try:
            # Таблица для общих частот символов
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS overall_frequencies (
                    symbol VARCHAR(10) PRIMARY KEY,
                    frequency BIGINT NOT NULL
                )
            ''')

            # Таблицы для условных вероятностей (от 1 до 13 предыдущих символов)
            for n in range(1, 14):
                self.cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS markov_{n} (
                        context VARCHAR(50) NOT NULL,
                        symbol VARCHAR(10) NOT NULL,
                        frequency BIGINT NOT NULL,
                        PRIMARY KEY (context, symbol)
                    )
                ''')

                # Создаем индекс для быстрого поиска по контексту
                self.cursor.execute(f'''
                    CREATE INDEX IF NOT EXISTS idx_markov_{n}_context 
                    ON markov_{n} (context)
                ''')

            self.conn.commit()
            logger.info("Таблицы созданы успешно")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка создания таблиц: {e}")
            raise

    def save_overall_frequencies(self, frequencies):
        #Сохранение общих частот символов
        try:
            # Очищаем таблицу
            self.cursor.execute('TRUNCATE TABLE overall_frequencies RESTART IDENTITY')

            # Пакетная вставка
            data = [(symbol, freq) for symbol, freq in frequencies.items()]
            execute_batch(self.cursor,
                          "INSERT INTO overall_frequencies (symbol, frequency) VALUES (%s, %s)",
                          data
                          )

            self.conn.commit()
            logger.info(f"Сохранено {len(data)} общих частот")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка сохранения общих частот: {e}")
            raise

    def save_markov_frequencies(self, n, frequencies):
        # Сохранение частот для n-грамм с оптимизацией для больших объемов
        try:
            table_name = f'markov_{n}'

            # Очищаем таблицу
            self.cursor.execute(f'TRUNCATE TABLE {table_name} RESTART IDENTITY')

            # Подготавливаем данные для пакетной вставки
            data = []
            for context, symbols in frequencies.items():
                for symbol, freq in symbols.items():
                    data.append((context, symbol, freq))

            # Вставляем пачками по 10000 записей
            batch_size = 10000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                execute_batch(self.cursor,
                              f"INSERT INTO {table_name} (context, symbol, frequency) VALUES (%s, %s, %s)",
                              batch
                              )
                self.conn.commit()
                logger.info(f"Порядок {n}: сохранено {min(i + batch_size, len(data))}/{len(data)} записей")

            logger.info(f"Порядок {n}: всего сохранено {len(data)} записей")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка сохранения markov_{n}: {e}")
            raise

    def save_markov_frequencies_bulk(self, n, frequencies):
        try:
            table_name = f'markov_{n}'

            # Очищаем таблицу
            self.cursor.execute(f'TRUNCATE TABLE {table_name} RESTART IDENTITY')

            # Создаем временный файл
            import tempfile
            import csv

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
                writer = csv.writer(f)
                for context, symbols in frequencies.items():
                    for symbol, freq in symbols.items():
                        writer.writerow([context, symbol, freq])
                temp_file_path = f.name

            # Копируем из CSV файла
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                self.cursor.copy_expert(
                    f"COPY {table_name} (context, symbol, frequency) FROM STDIN WITH CSV",
                    f
                )

            self.conn.commit()

            # Удаляем временный файл
            import os
            os.unlink(temp_file_path)

            logger.info(f"Порядок {n}: bulk сохранено {self.cursor.rowcount} записей")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка bulk сохранения markov_{n}: {e}")
            raise

    def get_overall_probabilities(self):
        # Получение общих вероятностей символов
        try:
            self.cursor.execute('SELECT symbol, frequency FROM overall_frequencies')
            rows = self.cursor.fetchall()
            total = sum(freq for _, freq in rows)

            probabilities = {}
            for symbol, freq in rows:
                probabilities[symbol] = freq / total if total > 0 else 0

            return probabilities

        except Exception as e:
            logger.error(f"Ошибка получения общих вероятностей: {e}")
            return {}

    def get_markov_probabilities(self, n, context):
        # Получение условных вероятностей для заданного контекста
        try:
            table_name = f'markov_{n}'
            self.cursor.execute(
                f'SELECT symbol, frequency FROM {table_name} WHERE context = %s',
                (context,)
            )
            rows = self.cursor.fetchall()

            if not rows:
                return {}

            total = sum(freq for _, freq in rows)
            probabilities = {}

            for symbol, freq in rows:
                probabilities[symbol] = freq / total if total > 0 else 0

            return probabilities

        except Exception as e:
            logger.error(f"Ошибка получения вероятностей markov_{n}: {e}")
            return {}

    def get_database_stats(self):
        #Получение статистики по базе данных
        stats = {}
        try:
            # Размер базы данных
            self.cursor.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database()))
            """)
            stats['db_size'] = self.cursor.fetchone()[0]

            # Количество записей в каждой таблице
            for n in range(1, 14):
                self.cursor.execute(f"SELECT COUNT(*) FROM markov_{n}")
                stats[f'markov_{n}_count'] = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(*) FROM overall_frequencies")
            stats['overall_count'] = self.cursor.fetchone()[0]

        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")

        return stats

    def close(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
            logger.info("Подключение к PostgreSQL закрыто")