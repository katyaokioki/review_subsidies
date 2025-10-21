import pdfplumber
import pandas as pd
import re
import os
from pathlib import Path
from loguru import logger
import pika
from itertools import groupby
from operator import itemgetter
# df = pd.read_excel('downloads/Реестр отборов.xlsx')

# # Маска "description не пустой"
# mask = df['description'].notna() & (df['description'].astype(str).str.strip() != '')

# # Если столбец называется "Шифр" (или "шифр")
# ciphers = df.loc[mask, 'Шифр отбора']  # замените на точное имя колонки
# print(ciphers.tolist())


def _iter_lines_from_words(page, y_tol=2):
    """
    Собирает строки из слов по координате Y.
    Возвращает список строк в порядке чтения.
    """
    words = page.extract_words(use_text_flow=True, keep_blank_chars=False, extra_attrs=["top","x0"])
    # Сортируем по Y и X
    words.sort(key=lambda w: (round(w['top']/y_tol), w['x0']))
    for _, grp in groupby(words, key=lambda w: round(w['top']/y_tol)):
        row = sorted(list(grp), key=itemgetter('x0'))
        yield " ".join(w['text'] for w in row).strip()

def parse_pdf_description(pdf_path):
    target = "полное описание отбора"
    collecting = False
    desc_lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for line in _iter_lines_from_words(page, y_tol=2):
                low = line.lower()

                # старт: нашли строку с "полное описание отбора"
                if not collecting and target in low:
                    collecting = True
                    continue  # начинать со следующей строки

                if collecting:
                    # стоп по заголовку вида "^\d+\.\s"
                    if re.match(r'^\s*\d+\.\s', line):
                        collecting = False
                        break  # переходим к следующей странице/выходим
                    # накапливаем описание
                    desc_lines.append(line)

            # если уже собрали и встретили стоп — можно выйти
            if not collecting and desc_lines:
                break

    return " ".join(desc_lines).strip()

a = parse_pdf_description('./subsidies/25-117-07270-2-0046.pdf')
print(a)