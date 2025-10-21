import pdfplumber
import pandas as pd
import re
import os
from pathlib import Path
from loguru import logger
import pika
from itertools import groupby
from operator import itemgetter
import pymupdf
from sqlalchemy import create_engine, MetaData, Table, Column, Text, inspect, text


class Updated:
    def __init__(self):
        self.conn_str = "postgresql+psycopg2://postgres:1234@db:5432/subsidies"
        self.schema = "public"
        self.table = "main_table"

    def upsert(self, description: str, requirements: str, code: str):
        engine = create_engine(self.conn_str, future=True)
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
        with engine.begin() as conn:
            res = conn.execute(sql, params)
            logger.info("updated:", res.rowcount)


def add_links_to_dataframe(df, links_dict, id_column='Шифр отбора', link_column='ссылка'):
    """
    Добавляет ссылки из словаря в DataFrame
    
    Args:
        df: DataFrame с данными
        links_dict: словарь {шифр: ссылка}
        id_column: название столбца с шифрами
        link_column: название столбца для ссылок
    
    Returns:
        DataFrame с добавленными ссылками
    """
    # Создаем копию DataFrame
    result_df = df.copy()
    
    # Добавляем столбец для ссылок, если его нет
    if link_column not in result_df.columns:
        result_df[link_column] = ""
    
    # Добавляем ссылки из словаря
    for index, row in result_df.iterrows():
        current_id = str(row[id_column]).strip()
        if current_id in links_dict:
            result_df.at[index, link_column] = links_dict[current_id]
            logger.info(f"Добавлена ссылка для шифра {current_id}: {links_dict[current_id]}")
    
    return result_df

def parse_pdf_requirements(pdf_path):
    """
    Извлекает требования из PDF файла
    Returns:
        list: список требований
    """
    requirements = []
    collect = False
    req_buf = []
    word_buffer = []  # Буфер для поиска последовательности слов
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for obj in page.extract_words(extra_attrs=["fontname"]):
                current_line = obj["text"]
                font = obj["fontname"]
                
                # Добавляем слово в буфер для поиска фразы
                word_buffer.append(current_line.lower())
                if len(word_buffer) > 4:  # Держим только последние 4 слова
                    word_buffer.pop(0)
                
                # Проверяем, есть ли фраза "требования к участникам отбора"
                phrase = " ".join(word_buffer)
                if "требования к участникам отбора" in phrase and not collect:
                    collect = True
                    req_buf = []
                    print(f'!!! НАЙДЕНА ФРАЗА: "{phrase}"')
                
                # Останавливаем сбор по жирному шрифту
                if collect and font == 'TimesNewRomanPS-BoldMT':
                    # Сохраняем собранное, если не пусто
                    if req_buf:
                        requirements.append(" ".join(req_buf))
                        print(f'!!! СОХРАНЕНО: {" ".join(req_buf)}')
                    collect = False
                    req_buf = []
                elif collect:
                    req_buf.append(current_line)

        # Проверяем, если в конце что-то осталось
        if collect and req_buf:
            requirements.append(" ".join(req_buf))
            print(f'!!! СОХРАНЕНО В КОНЦЕ: {" ".join(req_buf)}')

    print(f"Найдено требований: {len(requirements)}")
    return requirements

def split_requirements_by_sentences(text):
    """Разделяет текст требований на предложения по заглавным буквам"""
    
    # Убираем лишний текст в начале
    text = re.sub(r'^.*?Соответствие', 'Соответствие', text, flags=re.DOTALL)
    
    # Очищаем от технических деталей
    text = re.sub(r'Отбор \d+-\d+-\d+-\d+-\d+', '', text)
    text = re.sub(r'Страница \d+ из \d+', '', text)
    text = re.sub(r'Дополнительная информация', '', text)
    
    # Разделяем текст на предложения по точкам
    sentences = re.split(r'\.\s+', text)
    
    # Фильтруем и очищаем предложения
    filtered_sentences = []
    for sentence in sentences:
        sentence = sentence.strip()
        
        # Пропускаем пустые и слишком короткие предложения
        if len(sentence) < 20:
            continue
            
        # Пропускаем технические детали
        if (sentence.startswith('Отбор') or 
            sentence.startswith('Страница') or
            sentence.startswith('Дополнительная') or
            'Шкала оценок' in sentence or
            'Весовое значение' in sentence or
            'Методология оценки' in sentence or
            sentence.startswith('3.') or
            sentence.startswith('Информация')):
            continue
        
        # Проверяем, что предложение начинается с заглавной буквы ИЛИ содержит ключевые слова
        if (sentence and sentence[0].isupper()) or any(keyword in sentence for keyword in [
            'Участник отбора', 'участник отбора', 'отсутствует', 'не является', 
            'не находится', 'не получает', 'предоставил', 'В реестре'
        ]):
            # Исключаем предложения, начинающиеся с "Российская" или "Федерация"
            if not sentence.startswith('Российская') and not sentence.startswith('Федерация'):
                # Дополнительно разделяем длинные предложения по ключевым словам
                if len(sentence) > 200:
                    # Разделяем по ключевым словам
                    parts = re.split(r'(?=Участник отбора|участник отбора|У участника|В реестре)', sentence)
                    for part in parts:
                        part = part.strip()
                        if len(part) > 20:
                            filtered_sentences.append(part)
                else:
                    filtered_sentences.append(sentence)
    
    return filtered_sentences



def parse_pdf_description(pdf_path):
    start_marker = "полное описание отбора"

    # 1) Считываем текст всего документа
    doc = pymupdf.open(pdf_path)  # PyMuPDF
    full_text = []
    for page in doc:
        full_text.append(page.get_text("text"))  # плоский текст в естественном порядке
    text = "\n".join(full_text)

    # 2) Находим позицию старта: сразу после маркера
    start_pos = text.lower().find(start_marker.lower())
    if start_pos == -1:
        raise ValueError(f"Не найдено начало по маркеру: {start_marker!r}")

    start_pos = start_pos + len(start_marker)

    # 3) Обрезаем текст от старта и ищем первую строку вида "число.текст"
    #   - ^\d+\..*$  — строка начинается с одного или более цифр, затем точка, затем любой текст
    #   - re.MULTILINE  — ^ и $ работают построчно
    #   - re.DOTALL     — на случай, если потребуется матчить через переводы строк (для поиска конца используем только MULTILINE)
    tail = text[start_pos:]

    pattern = re.compile(r"^\d+\..*$", flags=re.MULTILINE)
    m = pattern.search(tail)
    if not m:
        # Если такого заголовка нет — забираем всё до конца
        extracted = tail.strip()
    else:
        end_pos = m.start()
        extracted = tail[:end_pos].strip()
    extracted = " ".join(extracted.split())
    return extracted
 
def get_pdf_files(code: str) -> Path | None:
    """
    Возвращает путь к файлу /app/subsidies/<code>.pdf, если он существует.
    Иначе возвращает None.
    """
    SUBSIDIES_DIR = '/app/subsidies'
    path = Path(SUBSIDIES_DIR) / f"{code}.pdf"
    return path if path.is_file() else None

def parser_main(code):
    df = pd.read_excel("/app/downloads/Реестр отборов.xlsx") 
    
    
    # Добавляем столбец requirements, если его нет
    if 'requirements' not in df.columns:
        df['requirements'] = ""
        print("✅ Добавлен столбец requirements")

    if 'description' not in df.columns:
        df['description'] = ""
        print("✅ Добавлен столбец requirements")
    
    # Получаем список всех PDF файлов
    pdf_path = get_pdf_files(code)
    
    if not pdf_path:
        print("В папке subsidies нет PDF файлов")
        return
    
    print(f"Найдено PDF файлов: {pdf_path}")
    
    # Обрабатываем каждый PDF файл
    
        # Получаем имя файла без расширения
    pdf_name = Path(pdf_path).stem  # убираем .pdf
    print(f"Обрабатываем файл: {pdf_path}")
    print(f"Ищем строку с шифром: {pdf_name}")
        
    # Ищем строку с соответствующим шифром отбора
    matching_rows = df[df['Шифр отбора'] == code]
        
    if matching_rows.empty:
        print(f"⚠️  Не найдена строка с шифром отбора: {pdf_name}")
        
        
    print(f"✅ Найдена строка для шифра: {pdf_name}")
        
        # Извлекаем требования из PDF
    requirements = parse_pdf_requirements(pdf_path)
    description = parse_pdf_description(pdf_path)
    print(description)
        
    # Находим индекс строки с соответствующим шифром
    row_index = df[df['Шифр отбора'] == code].index[0]
        
    # # Получаем структурированные требования
    if len(requirements) > 0:
    #     structured_requirements = split_requirements_by_sentences(requirements[0])
    #     print(f"Структурировано требований: {len(structured_requirements)}")
            
        # Записываем требования в соответствующую строку
    #     df.at[row_index, 'requirements'] = requirements
    #     print(f"✅ Требования записаны в строку с шифром: {pdf_name}")
    # else:
    #     print(f"⚠️  Требования не найдены в файле: {pdf_path}")
    # if len(description) > 0:
        
    #     # Записываем требования в соответствующую строку
    #     df.at[row_index, 'description'] = description
    #     print(f"✅ описание записаны в строку с шифром: {pdf_name}")
    # else:
    #     print(f"⚠️  описание не найдено в файле: {pdf_path}")
        try:
            up = Updated()
            up.upsert(description, requirements, code)
            logger.info('успешная вставка')
        except:
            logger.info('не получилось description and requirements вставить')
        

    



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
        self.channel.queue_declare(queue='send_email', durable=True)
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
            parser_main(code)
            req_log.info("Processed successfully")
            ch.basic_publish(
                exchange='',
                routing_key='send_email',
                body=code.encode(),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent  # сохранить на диск
                )
            )
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
