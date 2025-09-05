import os
import shutil
from pathlib import Path

def clean_subsidies_folder():
    """
    Очищает папку subsidies от файлов, которые не являются PDF
    """
    subsidies_path = 'subsidies'
    subsidies_dir = Path(subsidies_path)
    
    if not subsidies_dir.exists():
        print(f"Папка {subsidies_path} не существует")
        return
    
    print(f"Очистка папки {subsidies_path}...")
    
    # Счетчики
    pdf_files = 0
    non_pdf_files = 0
    removed_files = 0
    
    # Проходим по всем файлам в папке
    for item in subsidies_dir.iterdir():
        if item.is_file():
            if item.suffix.lower() == '.pdf':
                pdf_files += 1
                print(f"✓ PDF файл: {item.name}")
            else:
                non_pdf_files += 1
                print(f"✗ Не-PDF файл: {item.name}")
                try:
                    # Удаляем не-PDF файл
                    item.unlink()
                    removed_files += 1
                    print(f"  → Удален: {item.name}")
                except Exception as e:
                    print(f"  → Ошибка при удалении {item.name}: {e}")
        elif item.is_dir():
            print(f"📁 Папка: {item.name}")
            # Рекурсивно очищаем подпапки
            clean_subfolder(str(item))
    
    print(f"\nРезультат очистки:")
    print(f"  PDF файлов: {pdf_files}")
    print(f"  Не-PDF файлов найдено: {non_pdf_files}")
    print(f"  Удалено файлов: {removed_files}")

def clean_subfolder(folder_path):
    """
    Рекурсивно очищает подпапку от не-PDF файлов
    """
    folder_dir = Path(folder_path)
    
    for item in folder_dir.iterdir():
        if item.is_file():
            if not item.suffix.lower() == '.pdf':
                try:
                    item.unlink()
                    print(f"  → Удален из подпапки: {item.name}")
                except Exception as e:
                    print(f"  → Ошибка при удалении {item.name}: {e}")
        elif item.is_dir():
            clean_subfolder(str(item))

