# requirements: sqlalchemy pandas openpyxl psycopg2-binary

from dataclasses import dataclass
from typing import Iterable, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, Column, String, UniqueConstraint
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from loguru import logger

# ---------- Конфигурация ----------

@dataclass(frozen=True)
class DbConfig:
    user: str = "postgres"
    password: str = "1234"
    host: str = "db"         # в Docker-сети — имя сервиса БД
    port: int = 5432         # внутренний порт Postgres в контейнере
    dbname: str = "subsidies"

    @property
    def conn_str(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


# ---------- БД и модель ----------

Base = declarative_base()

class Sample(Base):
    __tablename__ = "main_table"

    code = Column(String, primary_key=True)
    full_name = Column(String)
    org = Column(String)
    type_recipient = Column(String)
    selection_method = Column(String)
    page = Column(String)
    page_promout = Column(String)
    active_selectionss = Column(String)
    cofinancing = Column(String)
    cache_2022 = Column(String)
    cache_2023 = Column(String)  # nullable по умолчанию
    cache_2024 = Column(String)
    count_win = Column(String)
    max_cache = Column(String)
    start_date = Column(String)
    end_date = Column(String)
    win_date = Column(String)
    date_agreement = Column(String)
    link = Column(String)
    requirements = Column(String)

    __table_args__ = (
        UniqueConstraint("code", name="uq_samples_code"),
    )


class Db:
    def __init__(self):
        self.cfg = DbConfig(
            user="postgres",
            password="1234",
            host="db",
            port=5432,
            dbname="subsidies",
        )
        try:
            self.engine = create_engine(self.cfg.conn_str, future=True)
        except:
            logger.info('Соединения не получилось')

    def create_schema(self, drop_table: bool = False) -> None:
        # при необходимости очистить таблицу
        if drop_table:
            with self.engine.begin() as conn:
                conn.exec_driver_sql("DROP TABLE IF EXISTS main_table CASCADE")
        Base.metadata.create_all(self.engine)

    def session(self) -> Session:
        return Session(self.engine)


# ---------- Репозиторий (UPSERT) ----------

class SampleRepository:
    def __init__(self, db: Db):
        self.db = db

    def upsert_many(self, rows: Iterable[Dict[str, Any]]) -> int:
        """UPSERT по ключу code, возвращает число обработанных записей."""
        count = 0
        with self.db.session() as session:
            for data in rows:
                stmt = pg_insert(Sample).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["code"],
                    set_={
                        "code": stmt.excluded.code,
                        "full_name": stmt.excluded.full_name,
                        "org": stmt.excluded.org,
                        "type_recipient": stmt.excluded.type_recipient,
                        "selection_method": stmt.excluded.selection_method,
                        "page": stmt.excluded.page,
                        "page_promout": stmt.excluded.page_promout,
                        "active_selectionss": stmt.excluded.active_selectionss,
                        "cofinancing": stmt.excluded.cofinancing,
                        "cache_2022": stmt.excluded.cache_2022,
                        "cache_2023": stmt.excluded.cache_2023,
                        "cache_2024": stmt.excluded.cache_2024,
                        "count_win": stmt.excluded.count_win,
                        "max_cache": stmt.excluded.max_cache,
                        "start_date": stmt.excluded.start_date,
                        "end_date": stmt.excluded.end_date,
                        "win_date": stmt.excluded.win_date,
                        "date_agreement": stmt.excluded.date_agreement,
                    },
                )
                session.execute(stmt)
                count += 1
            session.commit()
        return count


# ---------- Загрузчик Excel ----------

class ExcelLoader:
    # соответствие исходных русских колонок полям модели
    COLS = [
        "Шифр отбора",
        "Полное наименование отбора",
        "Организация, предоставляющая субсидию",
        "Тип получателей",
        "Способ проведения отбора",
        "Отбор проводится на Портале",
        "Сайт проведения отбора",
        "Активные отборы (прием заявок)",
        "Софинансирование",
        "Объем распределяемых средств 2022, ₽",
        "Объем распределяемых средств 2023, ₽",
        "Объем распределяемых средств 2024, ₽",
        "Количество победителей отбора",
        "Предельный размер субсидии для одного получателя, ₽",
        "Дата начала приема заявок",
        "Дата окончания приема заявок",
        "Дата определения победителей",
        "Дата заключения соглашений",
    ]

    RENAME = {
        "Шифр отбора": "code",
        "Полное наименование отбора": "full_name",
        "Организация, предоставляющая субсидию": "org",
        "Тип получателей": "type_recipient",
        "Способ проведения отбора": "selection_method",
        "Отбор проводится на Портале": "page",
        "Сайт проведения отбора": "page_promout",
        "Активные отборы (прием заявок)": "active_selectionss",
        "Софинансирование": "cofinancing",
        "Объем распределяемых средств 2022, ₽": "cache_2022",
        "Объем распределяемых средств 2023, ₽": "cache_2023",
        "Объем распределяемых средств 2024, ₽": "cache_2024",
        "Количество победителей отбора": "count_win",
        "Предельный размер субсидии для одного получателя, ₽": "max_cache",
        "Дата начала приема заявок": "start_date",
        "Дата окончания приема заявок": "end_date",
        "Дата определения победителей": "win_date",
        "Дата заключения соглашений": "date_agreement",
    }

    def __init__(self, excel_path: str):
        self.excel_path = excel_path

    def load(self) -> pd.DataFrame:
        sheets = pd.read_excel(self.excel_path, sheet_name=None)
        df = pd.concat(sheets.values(), ignore_index=True)
        df = df[self.COLS].rename(columns=self.RENAME)
        # NaN -> None для корректной вставки
        df = df.where(pd.notna(df), None)
        return df

    def as_dicts(self) -> Iterable[Dict[str, Any]]:
        df = self.load()
        for _, row in df.iterrows():
            yield row.to_dict()


# ---------- Сценарий запуска ----------

class Runner:
    def __init__(self, excel_path: str):
        self.db = Db()
        self.repo = SampleRepository(self.db)
        try:
            self.loader = ExcelLoader(excel_path)
        except:
            logger.info('Не получилось считать таблицу')

    def run(self, drop_table: bool = False) -> int:
        self.db.create_schema(drop_table=drop_table)
        rows = self.loader.as_dicts()
        return self.repo.upsert_many(rows)


# if __name__ == "__main__":
#     cfg = DbConfig(
#         user="postgres",
#         password="1234",
#         host="db",
#         port=5432,
#         dbname="subsidies",
#     )
#     excel_path = "Реестр отборов.xlsx"

#     runner = Runner(excel_path)
#     total = runner.run(drop_table=False)  # поставьте True, если нужно пересоздать таблицу
#     print(f"Upserted rows: {total}")
