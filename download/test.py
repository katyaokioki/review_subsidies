import os
import random
import re
import shutil
import time
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from loguru import logger
import pika

# Настройка логгера: ротация логов, хранение 7 дней, сжатие
logger.add("logs/parser_{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days", compression="zip")


class BudgetDocDownloader:
    USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:79.0) Gecko/20100101 Firefox/79.0'
]
    
    def __init__(self, code, download_folder="./subsidies"):
        self.code = code
        self.download_folder = os.path.abspath(download_folder)
        self.shifr = {}
    
    def get_random_user_agent(self):
        return random.choice(self.USER_AGENTS)

    def random_sleep(self, a=1, b=3):
        t = random.uniform(a, b)
        logger.debug(f"Sleeping for {t:.2f} seconds")
        time.sleep(t)

    def init_driver(self):
        chrome_options = Options()
        user_agent = self.get_random_user_agent()
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Remote(
            command_executor=os.getenv("SELENIUM_URL", "http://selenium-chrome:4444/wd/hub"),
            options=chrome_options
        )

        wait = WebDriverWait(driver, 10)
        logger.info("WebDriver и WebDriverWait инициализированы")
        return driver, wait
    
    def go_back(self, driver):
        driver.back()

    def insert(self, driver, wait, code):
        try:
            input_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "minfin-search__input")))
            input_element.clear()
            input_element.send_keys(code)
            logger.info(f"Вставлен код {code} в поисковое поле")
            return True
        except Exception as e:
            logger.error(f"Ошибка вставки кода {code} в поисковое поле: {e}")
            return False

    def click_element(self, wait, driver, css_selector):
        # try:
        #     wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".overlay-class")))
        # except TimeoutException:
        #     logger.warning(f"Проблемы с overlay, селектор: {css_selector}")
        #     return False
        try:
            element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            element.click()
            logger.info(f"Клик по элементу {css_selector} выполнен")
            current_url = driver.current_url
            return current_url
        except (TimeoutException, WebDriverException):
            logger.error(f"Ошибка при клике по элементу {css_selector}: {e}")
            return False
            # try:
            #     driver.execute_script("arguments[0].click();", element)
            #     logger.info(f"Клик по элементу {css_selector} выполнен через JS")
            #     current_url = driver.current_url
            #     return current_url
            # except Exception as e:
            #     logger.error(f"Клик через JS не удался для {css_selector}: {e}")
            #     return False

    def get_href(self, wait, selector):
        try:
            link_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
            href = link_element.get_attribute('href')
            logger.info(f"Ссылка успешно получена: {href}")
            return href
        except Exception as e:
            logger.error(f"Ошибка при получении ссылки по селектору {selector}: {e}")
            return None

    def get_treb(self, driver, wait):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.minfin-accordion__head.ng-tns-c2067363559-1"))
            )
            elements = driver.find_elements(By.CSS_SELECTOR, "div.minfin-accordion__head.ng-tns-c2067363559-1")
            for element in elements:
                if 'Объявления и протоколы' in element.text:
                    element.click()
                    logger.info("Кликнули на 'Объявления и протоколы'")
                    return True
            logger.warning("Элемент 'Объявления и протоколы' не найден")
            return False
        except Exception as e:
            logger.error(f"Ошибка при клике на 'Объявления и протоколы': {e}")
            return False

    def download_doc(self, wait, driver):
        try:
            link_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), 'Объявление об отборе')]")))
            for link_element in link_elements:
                text = link_element.text
                if re.match(r'^Объявление об отборе.*', text):
                    href = link_element.get_attribute('href')
                    logger.info(f"Найдена ссылка на документ: {href}")
                    return href
            logger.warning("Ссылка на документ не найдена")
            return None
        except Exception as e:
            logger.error(f"Ошибка при поиске ссылки на документ: {e}")
            return None

    def downl(self, url, filename):
        headers = {'User-Agent': self.get_random_user_agent()}
        try:
            response = requests.get(url, stream=True, headers=headers)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Файл успешно скачан: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Ошибка при скачивании файла {url}: {e}")
            return False

    def move_all_pdfs(self, source_dir, filename):
        try:
            os.makedirs(self.download_folder, exist_ok=True)
            for name in os.listdir(source_dir):
                if name.lower() == filename.lower():
                    src_path = os.path.join(source_dir, name)
                    base, ext = os.path.splitext(name)
                    new_name = base + '.pdf' if ext.lower() != '.pdf' else name
                    dst_path = os.path.join(self.download_folder, new_name)
                    if os.path.isfile(src_path):
                        shutil.move(src_path, dst_path)
                        logger.info(f"Перемещён файл: {name} -> {new_name}")
        except Exception as e:
            logger.error(f"Ошибка при перемещении файлов: {e}")

    def safe_quit(self, driver):
        try:
            if driver:
                driver.quit()
                logger.info("Веб-драйвер корректно закрыт")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии веб-драйвера: {e}")

    def run(self):
        driver, wait = self.init_driver()
        status = True

        try:
            driver.get('https://promote.budget.gov.ru/')
            logger.info('Открыли https://promote.budget.gov.ru/')
        except Exception as e:
            logger.error(f"Не удалось открыть сайт: {e}")
            status = False

        if status and self.insert(driver, wait, self.code):
            if not self.click_element(wait, driver, 'button.mrx-btn.btn.icon-left.minfin-search__btn.mrx-btn-lg.mrx-btn-primary.ng-star-inserted'):
                logger.error('Не удалось нажать кнопку поиска.')
                status = False
            href = driver.current_url
            logger.info(f"Текущий URL: {href}")
            
        if status:
            current_url = driver.current_url
            logger.info(f"Текущий URL: {current_url}")
            href = self.get_href(wait, "a.selection-head")
            if not(href):
                driver.back()
                current_url = driver.current_url
                logger.info(f"Текущий URL: {current_url}")
                


            # for i in range(3):
            #     self.random_sleep(1, 2)
            #     try:
            #         href = self.get_href(wait, "a.selection-head")
            #         break
            #     except Exception as e:
            #         logger.error(f"Ошибка при получении ссылки: {e}")
            #         self.go_back(driver)
            #         status = False


            if href:
                try:
                    driver.get(href)
                    logger.info(f"Перешли по ссылке: {href}")
                except Exception as e:
                    logger.error(f"Ошибка при переходе по ссылке {href}: {e}")
                    status = False

        self.random_sleep(7, 10)
        if status and self.click_element(wait, driver, 'div.competition-head-more'):
            logger.info('Кликнули Подробнее')
            self.shifr[self.code] = driver.current_url

        self.random_sleep(5, 7)
        logger.debug(f"Текущий URL: {driver.current_url}")

        if self.get_treb(driver, wait):
            logger.info('Нажали на документы')
            self.random_sleep(2, 4)

        self.random_sleep(2, 5)
        if status:
            href = None
            for _ in range(3):
                href = self.download_doc(wait, driver)
                self.random_sleep(1, 2)
                if href:
                    break

            if href and status:
                filename = self.downl(href, self.code)
                if filename:
                    self.move_all_pdfs(os.path.expanduser('./'), filename)
                else:
                    logger.warning('Не удалось получить ссылку на документ')
                    status = False

        self.safe_quit(driver)
        return self.shifr


class Link_to_excel:
    def __init__(self):
        self.df = pd.read_excel('Реестр отборов.xlsx')

    def set_link(self, code, link):
        self.df.loc[self.df['Шифр отбора'] == code, 'ссылка'] = link
        logger.info(f"Установлена ссылка для кода {code}: {link}")

    def save(self, filename='Реестр отборов.xlsx'):
        self.df.to_excel(filename, index=False)
        logger.info(f"Файл сохранён: {filename}")



class ReadingRabbitMQ:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', heartbeat=60))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='codes', durable=True)
        self.channel.queue_declare(queue='error_codes', durable=True)
        self.channel.queue_declare(queue='parser_pdf', durable=True)

    def callback(self, ch, method, properties, body):
        code = body.decode()
        logger.info(f"Получено сообщение: {code}")

        try:
            downloader = BudgetDocDownloader(code, './subsidies')
            result = downloader.run()
            link_to_excel = Link_to_excel()
            link_to_excel.set_link(code, result)
            link_to_excel.save()
            logger.info(f"Обработан код: {code}, результат: {result}")
            self.channel.basic_publish(exchange='', routing_key='parser_pdf', body=code)
        except Exception as e:
            logger.error(f"Ошибка при обработке кода {code}: {e}")
            self.channel.basic_publish(exchange='', routing_key='error_codes', body=code)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='codes', on_message_callback=self.callback, auto_ack=False)
        logger.info("Ожидание сообщений...")
        self.channel.start_consuming()
        self.connection.close()


if __name__ == "__main__":
    reader = ReadingRabbitMQ()
    reader.start_consuming()
