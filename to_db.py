import pandas as pd
from sqlalchemy import create_engine, text

class ExcelToPostgres:
    def __init__(self, db_url, table_name, unique_key):
        """
        db_url: строка подключения к PostgreSQL
        table_name: основная таблица для записи
        unique_key: поле или кортеж полей для обработки дублирования
        """
        self.engine = create_engine(db_url)
        self.table_name = table_name
        self.unique_key = unique_key
        self.df = None

    def load_excel(self, filepath):
        """Загрузить данные из Excel в DataFrame"""
        self.df = pd.read_excel(filepath)

    def sql_quote(self, column_name):
        """Оборачивает имя колонки в двойные кавычки"""
        return f'"{column_name}"'

    def write_to_db(self):
        """Записать данные с заменой дубликатов"""
        if self.df is None:
            raise ValueError("DataFrame пустой. Сначала загрузите Excel вызовом load_excel()")

        temp_table = f'{self.table_name}_temp'

        # Сохраняем в временную таблицу
        self.df.to_sql(temp_table, self.engine, if_exists='replace', index=False)

        # Оборачиваем имена колонок в кавычки
        columns_quoted = [self.sql_quote(col) for col in self.df.columns]

        # Формируем SET часть для обновления (кроме уникального ключа)
        if isinstance(self.unique_key, tuple):
            unique_keys = set(self.unique_key)
        else:
            unique_keys = set([self.unique_key])

        update_fields = [col for col in self.df.columns if col not in unique_keys]
        set_clause = ', '.join([f'{self.sql_quote(field)} = EXCLUDED.{self.sql_quote(field)}' for field in update_fields])

        # Формируем ON CONFLICT ключи
        if isinstance(self.unique_key, tuple):
            conflict_fields = ', '.join([self.sql_quote(k) for k in self.unique_key])
        else:
            conflict_fields = self.sql_quote(self.unique_key)

        query = text(f"""
            INSERT INTO {self.table_name} ({', '.join(columns_quoted)})
            SELECT {', '.join(columns_quoted)} FROM {temp_table}
            ON CONFLICT ({conflict_fields}) DO UPDATE SET
                {set_clause}
        """)

        with self.engine.begin() as conn:
            conn.execute(query)
            conn.execute(text(f"DROP TABLE {temp_table}"))

