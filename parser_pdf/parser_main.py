import pdfplumber
import pandas as pd
import re
import os
from pathlib import Path
from loguru import logger

# def add_links_to_dataframe(df, links_dict, id_column='Шифр отбора', link_column='ссылка'):
#     """
#     Добавляет ссылки из словаря в DataFrame
    
#     Args:
#         df: DataFrame с данными
#         links_dict: словарь {шифр: ссылка}
#         id_column: название столбца с шифрами
#         link_column: название столбца для ссылок
    
#     Returns:
#         DataFrame с добавленными ссылками
#     """
#     # Создаем копию DataFrame
#     result_df = df.copy()
    
#     # Добавляем столбец для ссылок, если его нет
#     if link_column not in result_df.columns:
#         result_df[link_column] = ""
    
#     # Добавляем ссылки из словаря
#     for index, row in result_df.iterrows():
#         current_id = str(row[id_column]).strip()
#         if current_id in links_dict:
#             result_df.at[index, link_column] = links_dict[current_id]
#             logger.info(f"Добавлена ссылка для шифра {current_id}: {links_dict[current_id]}")
    
#     return result_df

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

def get_pdf_files(subsidies_path='subsidies'):
    """
    Получает список всех PDF файлов в папке subsidies
    """
    pdf_files = []
    subsidies_dir = Path(subsidies_path)
    
    if subsidies_dir.exists():
        for file in subsidies_dir.iterdir():
            if file.is_file() and file.suffix.lower() == '.pdf':
                pdf_files.append(str(file))
    
    return pdf_files

def parser_main(a):
    df = pd.read_excel("Реестр отборов.xlsx", sheet_name='2025 - 2027')

    
    # Добавляем столбец requirements, если его нет
    if 'requirements' not in df.columns:
        df['requirements'] = ""
        print("✅ Добавлен столбец requirements")
    
    # Получаем список всех PDF файлов
    pdf_files = get_pdf_files()
    
    if not pdf_files:
        print("В папке subsidies нет PDF файлов")
        return
    
    print(f"Найдено PDF файлов: {len(pdf_files)}")
    
    # Обрабатываем каждый PDF файл
    for pdf_path in pdf_files:
        # Получаем имя файла без расширения
        pdf_name = Path(pdf_path).stem  # убираем .pdf
        print(f"Обрабатываем файл: {pdf_path}")
        print(f"Ищем строку с шифром: {pdf_name}")
        
        # Ищем строку с соответствующим шифром отбора
        matching_rows = df[df['Шифр отбора'] == pdf_name]
        
        if matching_rows.empty:
            print(f"⚠️  Не найдена строка с шифром отбора: {pdf_name}")
            continue
        
        print(f"✅ Найдена строка для шифра: {pdf_name}")
        
        # Извлекаем требования из PDF
        requirements = parse_pdf_requirements(pdf_path)
        
        # Находим индекс строки с соответствующим шифром
        row_index = df[df['Шифр отбора'] == pdf_name].index[0]
        
        # Получаем структурированные требования
        if len(requirements) > 0:
            structured_requirements = split_requirements_by_sentences(requirements[0])
            print(f"Структурировано требований: {len(structured_requirements)}")
            
            # Записываем требования в соответствующую строку
            df.at[row_index, 'requirements'] = "\n".join(structured_requirements)
            print(f"✅ Требования записаны в строку с шифром: {pdf_name}")
        else:
            print(f"⚠️  Требования не найдены в файле: {pdf_path}")
    df = add_links_to_dataframe(df, a)
    df.to_excel('db.xlsx', index=False)
    print("✅ Результат сохранен в db.xlsx")
