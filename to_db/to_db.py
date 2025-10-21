import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateSchema
from loguru import logger
from sqlalchemy import create_engine, MetaData, Table, Column, Text, inspect
import sys




# Пример входного DataFrame: обязателен столбец 'code' + любые другие


class ExcelLoader:
    # def __init__(self, df: pd.DataFrame) -> None:
    #     self.combined = df


    def normalize_code_column(self, df: pd.DataFrame) -> pd.DataFrame:

        # Переименовать «Шифр отбора» -> code, если нужно
        if "code" not in df.columns and "Шифр отбора" in df.columns:
            df = df.rename(columns={"Шифр отбора": "code"})

        if "full_name" not in df.columns and "Полное наименование отбора" in df.columns:
            df = df.rename(columns={"Полное наименование отбора": "full_name"})
        if "org" not in df.columns and "Организация, предоставляющая субсидию" in df.columns:
            df = df.rename(columns={"Организация, предоставляющая субсидию": "org"})
        if "type_recipient" not in df.columns and "Тип получателей" in df.columns:
            df = df.rename(columns={"Тип получателей": "type_recipient"})
        if "selection_method" not in df.columns and "Способ проведения отбора" in df.columns:
            df = df.rename(columns={"Способ проведения отбора": "selection_method"})
        if "page" not in df.columns and "Отбор проводится на Портале" in df.columns:
            df = df.rename(columns={"Отбор проводится на Портале": "page"})
        if "page_promout" not in df.columns and "Сайт проведения отбора" in df.columns:
            df = df.rename(columns={"Сайт проведения отбора": "page_promout"})
        if "active_selectionss" not in df.columns and "Активные отборы (прием заявок)" in df.columns:
            df = df.rename(columns={"Активные отборы (прием заявок)": "active_selectionss"})
        if "cofinancing" not in df.columns and "Софинансирование" in df.columns:
            df = df.rename(columns={"Софинансирование": "cofinancing"})
        if "cache_2022" not in df.columns and "Объем распределяемых средств 2022, ₽" in df.columns:
            df = df.rename(columns={"Объем распределяемых средств 2022, ₽": "cache_2022"})
        if "cache_2023" not in df.columns and "Объем распределяемых средств 2023, ₽" in df.columns:
            df = df.rename(columns={"Объем распределяемых средств 2023, ₽": "cache_2023"})
        if "cache_2024" not in df.columns and "Объем распределяемых средств 2024, ₽" in df.columns:
            df = df.rename(columns={"Объем распределяемых средств 2024, ₽": "cache_2024"})
        if "cache_2025" not in df.columns and "Объем распределяемых средств 2025, ₽" in df.columns:
            df = df.rename(columns={"Объем распределяемых средств 2025, ₽": "cache_2025"})
        if "cache_2026" not in df.columns and "Объем распределяемых средств 2026, ₽" in df.columns:
            df = df.rename(columns={"Объем распределяемых средств 2026, ₽": "cache_2026"})
        if "cache_2027" not in df.columns and "Объем распределяемых средств 2027, ₽" in df.columns:
            df = df.rename(columns={"Объем распределяемых средств 2027, ₽": "cache_2027"})
        if "count_win" not in df.columns and "Количество победителей отбора" in df.columns:
            df = df.rename(columns={"Количество победителей отбора": "count_win"})
        if "max_cache" not in df.columns and "Предельный размер субсидии для одного получателя, ₽" in df.columns:
            df = df.rename(columns={"Предельный размер субсидии для одного получателя, ₽": "max_cache"})
        if "start_date" not in df.columns and "Дата начала приема заявок" in df.columns:
            df = df.rename(columns={"Дата начала приема заявок": "start_date"})
        if "end_date" not in df.columns and "Дата окончания приема заявок" in df.columns:
            df = df.rename(columns={"Дата окончания приема заявок": "end_date"})
        if "win_date" not in df.columns and "Дата определения победителей" in df.columns:
            df = df.rename(columns={"Дата определения победителей": "win_date"})
        if "date_agreement" not in df.columns and "Дата заключения соглашений" in df.columns:
            df = df.rename(columns={"Дата заключения соглашений": "date_agreement"})
       

        # df = self.combined.rename(columns=lambda c: RU2TARGET.get(c, c))  # русские -> целевые, остальное без изменений

        # # 3) избавляемся от любых суффиксов вида *_m123 или похожих — берём только целевые имена
        # df = df.loc[:, [c for c in df.columns if c in TARGET_COLUMNS or c in RU2TARGET.values()]]

        # # 4) гарантируем наличие целевых колонок (создаём пустые при отсутствии)
        # for col in TARGET_COLUMNS:
        #     if col not in df.columns:
        #         df[col] = None

        # # 5) упорядочиваем колонки как в БД
        # df = df[TARGET_COLUMNS]

        # # 6) очистка ключа
        # df["code"] = df["code"].astype(str).str.strip()

        return df


        







# Рекомендуется убедиться, что типы соответствуют БД, при необходимости сделать df.astype(...)
# Например:
# df = df.astype({"qty": "int64", "price": "float64"})
class Updated:
    def __init__(self):
        self.conn_str = "postgresql+psycopg2://postgres:1234@db:5432/subsidies"
        self.schema = "public"
        self.table = "main_table"
        self.engine = create_engine(self.conn_str, future=True)

    def db(self, df):
        temp_table = f"_{self.table}_stg"
        # engine = create_engine(self.conn_str, future=True)
        if "code" not in df.columns:
            raise ValueError("В DataFrame отсутствует обязательный столбец 'code'")


        


        with self.engine.begin() as conn:
        # 1) Обеспечить уникальность по code (нужен уникальный индекс/constraint для ON CONFLICT)
            conn.execute(text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_indexes 
                        WHERE schemaname = :schema AND indexname = 'ux_{self.table}_code'
                    ) THEN
                        EXECUTE 'CREATE UNIQUE INDEX ux_{self.table}_code ON {self.schema}.{self.table}(code)';
                    END IF;
                END$$;
            """), {"schema": self.schema})

            conn.execute(text(f'DROP TABLE IF EXISTS "{self.schema}"."{temp_table}";'))
            conn.execute(text(f'CREATE TABLE "{self.schema}"."{temp_table}" (LIKE "{self.schema}"."{self.table}" INCLUDING ALL);'))


         
            df.to_sql(temp_table, conn, schema=self.schema, if_exists="append", index=False)

            cols = list(df.columns)
            non_pk_cols = [c for c in cols if c != "code"]
            set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_cols])
            upsert_sql = f"""
                INSERT INTO {self.schema}.{self.table} ({", ".join(cols)})
                SELECT {", ".join(cols)} FROM {self.schema}.{temp_table}
                ON CONFLICT (code) DO UPDATE
                SET {set_clause}
                RETURNING code, (xmax <> 0) AS updated;
            """
            res = conn.execute(text(upsert_sql))
            rows = res.all()
            new_codes = [r._mapping["code"] for r in rows if not r._mapping["updated"]]
            updated_codes = [r._mapping["code"] for r in rows if r._mapping["updated"]]
            logger.info(f"new={new_codes}, updated={len(updated_codes)}")
        return new_codes

    def upsert(self, description: str, requirements: str, code: str):
        # engine = create_engine(self.conn_str, future=True)
        sql = text("""
            UPDATE public.main_table
            SET description = :description,
                requirements = :requirements
            WHERE code = :code
        """)
        params = {
            "description": description,
            "requirements": requirements,
            "code": code,
        }
        with self.engine.begin() as conn:
            res = conn.execute(sql, params)
            logger.info("updated:", res.rowcount)



    def isactive(self, isactive, code):
        # engine = create_engine(self.conn_str, future=True)

        sql = text("""
            UPDATE public.main_table
            SET isactive = :isactive
            WHERE code = :code
        """)


        params = {
            "isactive": bool(isactive),
            "code": code,

        }
        with self.engine.begin() as conn:
            res = conn.execute(sql, params)
            logger.info("updated:", res.rowcount)


    
    def create_table(self):
        # engine = create_engine(self.conn_str, future=True)
        metadata = MetaData()
        with self.engine.connect() as conn:
            try:
                conn.execute(CreateSchema("public"))
            except:
                logger.info('')
        # Определение таблицы
        main_table = Table(
            "main_table",
            metadata,
            Column("code", Text),
            Column("full_name", Text),
            Column("org", Text),
            Column("type_recipient", Text),
            Column("selection_method", Text),
            Column("page", Text),
            Column("page_promout", Text),
            Column("active_selectionss", Text),
            Column("cofinancing", Text),
            Column("cache_2022", Text),
            Column("cache_2023", Text),
            Column("cache_2024", Text),
            Column("cache_2025", Text),
            Column("cache_2026", Text),
            Column("cache_2027", Text),
            Column("count_win", Text),
            Column("max_cache", Text),
            Column("start_date", Text),
            Column("end_date", Text),
            Column("win_date", Text),
            Column("date_agreement", Text),
            Column("link", Text),
            Column("description", Text),
            Column("requirements", Text),
            Column("sheet", Text),

            schema="public"
        )

        # Создание таблицы в базе данных
        metadata.create_all(self.engine)

    def check(self):
        engine = create_engine(self.conn_str, future=True)
        inspector = inspect(engine)
        if "main_table" in inspector.get_table_names(schema="public"):
            logger.info("Таблица main_table существует")
            return True
        else:
            logger.info("Таблица main_table не найдена")
            return False



