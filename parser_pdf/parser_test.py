# import os
# import re
# from pathlib import Path

# import pdfplumber
# import pandas as pd
# from loguru import logger

# # Абсолютные пути внутри контейнера
# SUBSIDIES_DIR = Path("/app/subsidies")          # где лежат PDF внутри контейнера [2]
# DOWNLOADS_DIR = Path("/app/downloads")          # где лежит Excel внутри контейнера [2]
# REGISTRY_FILE = DOWNLOADS_DIR / "Реестр отборов.xlsx"  # целевой Excel [1]

# logger.add("logs/parser_{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days", compression="zip")


# def parse_pdf_requirements(pdf_path):
#     """
#     Извлекает требования из PDF файла.
#     Возвращает список текстовых блоков требований.
#     """
#     requirements = []
#     collect = False
#     req_buf = []
#     word_buffer = []  # Буфер для поиска последовательности слов

#     with pdfplumber.open(pdf_path) as pdf:
#         for page in pdf.pages:
#             for obj in page.extract_words(extra_attrs=["fontname"]):
#                 current_line = obj["text"]
#                 font = obj["fontname"]

#                 # Добавляем слово в буфер для поиска фразы
#                 word_buffer.append(current_line.lower())
#                 if len(word_buffer) > 4:  # держим только последние 4 слова
#                     word_buffer.pop(0)

#                 # Проверяем, есть ли фраза "требования к участникам отбора"
#                 phrase = " ".join(word_buffer)
#                 if "требования к участникам отбора" in phrase and not collect:
#                     collect = True
#                     req_buf = []
#                     print(f'!!! НАЙДЕНА ФРАЗА: "{phrase}"')

#                 # Останавливаем сбор по жирному шрифту
#                 if collect and font == 'TimesNewRomanPS-BoldMT':
#                     if req_buf:
#                         requirements.append(" ".join(req_buf))
#                         print(f'!!! СОХРАНЕНО: {" ".join(req_buf)}')
#                     collect = False
#                     req_buf = []
#                 elif collect:
#                     req_buf.append(current_line)

#         # Проверяем, если в конце что-то осталось
#         if collect and req_buf:
#             requirements.append(" ".join(req_buf))
#             print(f'!!! СОХРАНЕНО В КОНЦЕ: {" ".join(req_buf)}')

#     print(f"Найдено требований: {len(requirements)}")
#     return requirements


# def split_requirements_by_sentences(text):
#     """Разделяет текст требований на предложения по заглавным буквам и фильтрует технический шум."""

#     # Убираем лишний текст в начале
#     text = re.sub(r'^.*?Соответствие', 'Соответствие', text, flags=re.DOTALL)

#     # Очищаем от технических деталей
#     text = re.sub(r'Отбор \d+-\d+-\d+-\d+-\d+', '', text)
#     text = re.sub(r'Страница \d+ из \d+', '', text)
#     text = re.sub(r'Дополнительная информация', '', text)

#     # Разделяем текст на предложения по точкам
#     sentences = re.split(r'\.\s+', text)

#     # Фильтруем и очищаем предложения
#     filtered_sentences = []
#     for sentence in sentences:
#         sentence = sentence.strip()

#         # Пропускаем пустые и слишком короткие предложения
#         if len(sentence) < 20:
#             continue

#         # Пропускаем технические детали
#         if (sentence.startswith('Отбор') or
#             sentence.startswith('Страница') or
#             sentence.startswith('Дополнительная') or
#             'Шкала оценок' in sentence or
#             'Весовое значение' in sentence or
#             'Методология оценки' in sentence or
#             sentence.startswith('3.') or
#             sentence.startswith('Информация')):
#             continue

#         # Проверяем, что предложение начинается с заглавной буквы ИЛИ содержит ключевые слова
#         if (sentence and sentence.isupper()) or any(keyword in sentence for keyword in [
#             'Участник отбора', 'участник отбора', 'отсутствует', 'не является',
#             'не находится', 'не получает', 'предоставил', 'В реестре'
#         ]):
#             # Исключаем предложения, начинающиеся с "Российская" или "Федерация"
#             if not sentence.startswith('Российская') and not sentence.startswith('Федерация'):
#                 # Дополнительно разделяем длинные предложения по ключевым словам
#                 if len(sentence) > 200:
#                     parts = re.split(r'(?=Участник отбора|участник отбора|У участника|В реестре)', sentence)
#                     for part in parts:
#                         part = part.strip()
#                         if len(part) > 20:
#                             filtered_sentences.append(part)
#                 else:
#                     filtered_sentences.append(sentence)

#     return filtered_sentences


# def get_pdf_files(subsidies_path=SUBSIDIES_DIR):
#     """
#     Получает список всех PDF файлов в /app/subsidies
#     """
#     pdf_files = []
#     subsidies_dir = Path(subsidies_path)

#     if subsidies_dir.exists():
#         for file in subsidies_dir.iterdir():
#             if file.is_file() and file.suffix.lower() == '.pdf':
#                 pdf_files.append(str(file))

#     return pdf_files


# def parser_main():
#     # Читаем Excel из /app/downloads/Реестр отборов.xlsx
#     if not REGISTRY_FILE.exists():
#         print(f"Файл реестра не найден: {REGISTRY_FILE}")
#         return

#     df = pd.read_excel(REGISTRY_FILE, sheet_name='2025 - 2027')  # абсолютный путь внутри контейнера [1]

#     # Добавляем столбец requirements, если его нет
#     if 'requirements' not in df.columns:
#         df['requirements'] = ""
#         print("✅ Добавлен столбец requirements")

#     # Получаем список всех PDF файлов из /app/subsidies
#     pdf_files = get_pdf_files()

#     if not pdf_files:
#         print("В /app/subsidies нет PDF файлов")
#         return

#     print(f"Найдено PDF файлов: {len(pdf_files)}")

#     # Обрабатываем каждый PDF файл
#     for pdf_path in pdf_files:
#         pdf_name = Path(pdf_path).stem  # имя файла без .pdf
#         print(f"Обрабатываем файл: {pdf_path}")
#         print(f"Ищем строку с шифром: {pdf_name}")

#         # Ищем строку с соответствующим шифром отбора
#         matching_rows = df[df['Шифр отбора'] == pdf_name]

#         if matching_rows.empty:
#             print(f"⚠️  Не найдена строка с шифром отбора: {pdf_name}")
#             continue

#         print(f"✅ Найдена строка для шифра: {pdf_name}")

#         # Извлекаем требования из PDF
#         requirements = parse_pdf_requirements(pdf_path)

#         # Находим индекс строки с соответствующим шифром
#         row_index = matching_rows.index

#         # Получаем структурированные требования
#         if requirements:
#             structured_requirements = split_requirements_by_sentences(requirements)
#             print(f"Структурировано требований: {len(structured_requirements)}")

#             # Записываем требования в соответствующую строку
#             df.at[row_index, 'requirements'] = "\n".join(structured_requirements)
#             print(f"✅ Требования записаны в строку с шифром: {pdf_name}")
#         else:
#             print(f"⚠️  Требования не найдены в файле: {pdf_path}")

#     # При необходимости можно сохранить результат рядом с исходным Excel
#     out_path = DOWNLOADS_DIR / "Реестр отборов (с требованиями).xlsx"
#     df.to_excel(out_path, index=False)
#     print(f"✅ Результат сохранен: {out_path}")


# if __name__ == "__main__":
#     parser_main()




import os
import re
import time
from pathlib import Path

import pika
import pdfplumber
import pandas as pd
from loguru import logger

# Пути внутри контейнера
SUBSIDIES_DIR = Path("/app/subsidies")
DOWNLOADS_DIR = Path("/app/downloads")
REGISTRY_FILE = DOWNLOADS_DIR / "Реестр отборов.xlsx"
OUTPUT_FILE = DOWNLOADS_DIR / "Реестр отборов (с требованиями).xlsx"

# Логгер
logger.add(
    "logs/parser_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="7 days",
    compression="zip",
    enqueue=True,  # безопасно при многопроцессной записи
    backtrace=True, diagnose=True  # полезные стеки
)

def parse_pdf_requirements(pdf_path: Path):
    ctx = logger.bind(stage="parse_pdf", pdf=str(pdf_path))
    t0 = time.perf_counter()
    requirements = []
    collect = False
    req_buf = []
    word_buffer = []
    pages = 0
    words_cnt = 0

    if not pdf_path.is_file():
        ctx.error("PDF file does not exist")
        return []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = len(pdf.pages)
            ctx.info(f"Opened PDF, pages={pages}")
            for i, page in enumerate(pdf.pages, start=1):
                page_t0 = time.perf_counter()
                # Можно добавить keep_blank_chars=True при необходимости
                words = page.extract_words(extra_attrs=["fontname"])
                words_cnt += len(words)
                ctx.debug(f"Page {i}: words={len(words)}")
                for obj in words:
                    current_line = obj["text"]
                    font = obj.get("fontname", "")
                    word_buffer.append(current_line.lower())
                    if len(word_buffer) > 4:
                        word_buffer.pop(0)

                    phrase = " ".join(word_buffer)
                    if "требования к участникам отбора" in phrase and not collect:
                        collect = True
                        req_buf = []
                        ctx.debug(f'Trigger phrase on page={i}: "{phrase}"')

                    if collect and font == 'TimesNewRomanPS-BoldMT':
                        if req_buf:
                            requirements.append(" ".join(req_buf))
                            ctx.debug(f"Saved block len={len(requirements[-1])}")
                        collect = False
                        req_buf = []
                    elif collect:
                        req_buf.append(current_line)

                ctx.debug(f"Page {i} parsed in {time.perf_counter()-page_t0:.3f}s")

            if collect and req_buf:
                requirements.append(" ".join(req_buf))
                ctx.debug(f"Saved trailing block len={len(requirements[-1])}")

        ctx.info(f"Parsed PDF blocks={len(requirements)}, total_words={words_cnt}, elapsed={time.perf_counter()-t0:.3f}s")
        return requirements
    except Exception as e:
        ctx.exception(f"Failed to parse PDF: {e}")
        return []

def split_requirements_by_sentences(text: str):
    ctx = logger.bind(stage="split_sentences")
    t0 = time.perf_counter()
    original_len = len(text)

    text = re.sub(r'^.*?Соответствие', 'Соответствие', text, flags=re.DOTALL)
    text = re.sub(r'Отбор \d+-\d+-\d+-\d+-\d+', '', text)
    text = re.sub(r'Страница \d+ из \d+', '', text)
    text = re.sub(r'Дополнительная информация', '', text)

    sentences = re.split(r'\.\s+', text)

    filtered_sentences = []
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 20:
            continue
        if (sentence.startswith('Отбор') or
            sentence.startswith('Страница') or
            sentence.startswith('Дополнительная') or
            'Шкала оценок' in sentence or
            'Весовое значение' in sentence or
            'Методология оценки' in sentence or
            sentence.startswith('3.') or
            sentence.startswith('Информация')):
            continue
        if (sentence and (sentence.isupper() or sentence.isupper())) or any(keyword in sentence for keyword in [
            'Участник отбора', 'участник отбора', 'отсутствует', 'не является',
            'не находится', 'не получает', 'предоставил', 'В реестре'
        ]):
            if not sentence.startswith('Российская') and not sentence.startswith('Федерация'):
                if len(sentence) > 200:
                    parts = re.split(r'(?=Участник отбора|участник отбора|У участника|В реестре)', sentence)
                    for part in parts:
                        part = part.strip()
                        if len(part) > 20:
                            filtered_sentences.append(part)
                else:
                    filtered_sentences.append(sentence)

    ctx.info(f"Split/filtered {len(filtered_sentences)} sentences from {original_len} chars in {time.perf_counter()-t0:.3f}s")
    return filtered_sentences

def get_pdf_path_for_code(code: str) -> Path | None:
    path = SUBSIDIES_DIR / f"{code}.pdf"
    logger.debug(f"Resolve PDF path for code={code} -> {path}")
    if path.is_file():
        logger.info(f"Found PDF for code={code}: {path}")
        return path
    logger.warning(f"PDF not found for code={code}: {path}")
    return None

def process_code(code: str):
    log = logger.bind(code=code, stage="process")
    t0 = time.perf_counter()

    # Проверка Excel
    if not REGISTRY_FILE.exists():
        log.error(f"Registry file not found: {REGISTRY_FILE}")
        raise FileNotFoundError(f"Registry file not found: {REGISTRY_FILE}")
    log.info(f"Reading Excel: {REGISTRY_FILE}")

    # PDF
    pdf_path = get_pdf_path_for_code(code)
    if not pdf_path:
        raise FileNotFoundError(f"PDF for code {code} not found in {SUBSIDIES_DIR}")

    # Чтение Excel
    try:
        df = pd.read_excel(REGISTRY_FILE, sheet_name='2025 - 2027')
        log.info(f"Excel loaded: rows={len(df)}, cols={list(df.columns)}")
    except Exception as e:
        log.exception(f"Failed to read Excel: {e}")
        raise

    if 'requirements' not in df.columns:
        df['requirements'] = ""
        log.debug("Added 'requirements' column")

    # Поиск строки
    matching_rows = df[df['Шифр отбора'] == code]
    if matching_rows.empty:
        log.error("Code not found in registry sheet")
        raise ValueError(f"Code not found in registry: {code}")
    row_index = matching_rows.index
    log.debug(f"Matched row index={row_index}")

    # Парсинг PDF
    blocks = parse_pdf_requirements(pdf_path)
    if not blocks:
        log.warning(f"No requirement blocks found in: {pdf_path.name}")
        structured = []
    else:
        structured = split_requirements_by_sentences(blocks)
        log.info(f"Structured sentences count={len(structured)}")

    df.at[row_index, 'requirements'] = "\n".join(structured)

    # Сохранение Excel
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(OUTPUT_FILE) as writer:
            df.to_excel(writer, index=False)
        log.info(f"Wrote result Excel: {OUTPUT_FILE}")
    except Exception as e:
        log.exception(f"Failed to write Excel: {e}")
        raise

    log.info(f"Done in {time.perf_counter()-t0:.3f}s for code={code}")

class RequirementsConsumer:
    def __init__(self, amqp_url: str = None):
        self.amqp_url = amqp_url or os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        logger.info(f"Connecting to RabbitMQ: {self.amqp_url}")
        try:
            params = pika.URLParameters(self.amqp_url)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            logger.info("RabbitMQ connection established")
        except Exception as e:
            logger.exception(f"RabbitMQ connection failed: {e}")
            raise

        # Очередь
        self.channel.queue_declare(queue='parser_pdf', durable=True)
        logger.info("Declared queue 'parser_pdf'")
        # QoS
        self.channel.basic_qos(prefetch_count=1)
        logger.info("Set basic_qos prefetch_count=1")

    def _on_message(self, ch, method, properties, body: bytes):
        code = body.decode(errors="ignore").strip()
        req_log = logger.bind(code=code, stage="consume")
        req_log.info("Received message")
        try:
            if not code:
                req_log.warning("Empty code received, ack and skip")
                return
            process_code(code)
            req_log.info("Processed successfully")
        except Exception as e:
            req_log.exception(f"Processing failed: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            req_log.debug("Ack sent to broker")

    def start(self):
        logger.info("Start consuming from 'parser_pdf'")
        self.channel.basic_consume(queue='parser_pdf', on_message_callback=self._on_message, auto_ack=False)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            self.channel.stop_consuming()
        except Exception as e:
            logger.exception(f"Consuming error: {e}")
            raise
        finally:
            try:
                self.connection.close()
                logger.info("RabbitMQ connection closed")
            except Exception as e:
                logger.warning(f"Connection close failed: {e}")

if __name__ == "__main__":
    RequirementsConsumer().start()
