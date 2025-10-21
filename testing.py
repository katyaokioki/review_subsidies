import fitz  # PyMuPDF
import pdfplumber


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
                        # print(f'!!! СОХРАНЕНО: {" ".join(req_buf)}')
                    collect = False
                    req_buf = []
                elif collect:
                    req_buf.append(current_line)

        # Проверяем, если в конце что-то осталось
        if collect and req_buf:
            requirements.append(" ".join(req_buf))
            # print(f'!!! СОХРАНЕНО В КОНЦЕ: {" ".join(req_buf)}')

    print(f"Найдено требований: {len(requirements)}")
    for i in requirements:
        line = i
    return line


text = parse_pdf_requirements('subsidies/25-117-07270-2-0046.pdf')
a = text.split('•')
print(a)

            

           
            

# "text", "blocks", "words" — по нужной детализаци





