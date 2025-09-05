# from download.test import download_main
from parser_pdf.parser_main import parser_main
from download.clean_subsidies import clean_subsidies_folder
from download.test import BudgetDocDownloader
from to_db import ExcelToPostgres





DB_URL = 'postgresql://postgres:1234@localhost:5432/subsidies'  # Заменить на свои данные
TABLE = 'main_table'  # Заменить на имя своей таблицы
UNIQUE_KEY = 'Шифр отбора'  # Заменить на уникальное поле или кортеж полей

def main():
    downloader = BudgetDocDownloader('Реестр отборов.xlsx')
    results = downloader.run()
    clean_subsidies_folder()
    parser_main(results)
    loader = ExcelToPostgres(DB_URL, TABLE, UNIQUE_KEY)
    loader.load_excel('db.xlsx') 
    loader.write_to_db()

if __name__ == "__main__":
    main()