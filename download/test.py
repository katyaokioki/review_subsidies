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
    
    def __init__(self, code, download_folder="/app/subsidies"):
        self.code = code
        self.download_folder = download_folder
        self.shifr = {}
        self.href = None
        os.makedirs(self.download_folder, exist_ok=True)  # создадим /app/subsidies, если нет [2][5]
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
    # def get_announcements_container_class(url: str, timeout: int = 20):
    #     # Запускаем Chrome (можно заменить на Remote/WebDriverManager по окружению)
    #     options = webdriver.ChromeOptions()
    #     options.add_argument("--headless=new")
    #     options.add_argument("--no-sandbox")
    #     options.add_argument("--disable-dev-shm-usage")
    #     driver = webdriver.Chrome(options=options)

    #     try:
    #         driver.get(url)

    #         # Находим сам <span> с нужным текстом
    #         span_xpath = "//span[normalize-space()='Объявления и протоколы']"
    #         wait = WebDriverWait(driver, timeout)
    #         span_el = wait.until(EC.presence_of_element_located((By.XPATH, span_xpath)))  # ждём появления [2]

    #         # Берем самый близкий div-родитель (не любой предок, а ближайший)
    #         # parent::div — это прямой родитель; если он не div, берём ближайший div-предок через ancestor::div[15]
    #         parent_div = None
    #         try:
    #             parent_div = span_el.find_element(By.XPATH, "parent::div")  # ближайший родитель, если это div [1]
    #         except Exception:
    #             parent_div = span_el.find_element(By.XPATH, "ancestor::div[15]")  # ближайший div‑предок [10][5]

    #         container_class = parent_div.get_attribute("class") or ""
    #         container_html = parent_div.get_attribute("outerHTML")  # для отладки структуры [1]

    #         return {"class": container_class.strip(), "html": container_html}
    #     except:
    #         logger.info(f'не получилось получить ссылку {url}')


    def get_treb(self, driver, wait):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.minfin-accordion__head.ng-tns-c1128774175-1"))
            )
            elements = driver.find_elements(By.CSS_SELECTOR, "div.minfin-accordion__head.ng-tns-c1128774175-1")
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

        # try:
        #     os.makedirs(os.path.dirname(filepath), exist_ok=True)  # на случай вложенных путей [2]
        #     with requests.get(url, stream=True, headers=headers,timeout=60) as response:
        #         response.raise_for_status()
        #         with open(filepath, 'wb') as f:
        #             for chunk in response.iter_content(chunk_size=8192):
        #                 if chunk:
        #                     f.write(chunk)
        #     logger.info(f"Файл успешно скачан: {filepath}")
        #     return filepath
        # except Exception as e:
        #     logger.error(f"Ошибка при скачивании файла {url} -> {filepath}: {e}")
        #     return False

    # def move_all_pdfs(self, source_dir, filename):
    #     try:
    #         os.makedirs(self.download_folder, exist_ok=True)
    #         for name in os.listdir(source_dir):
    #             if name.lower() == filename.lower():
    #                 src_path = os.path.join(source_dir, name)
    #                 base, ext = os.path.splitext(name)
    #                 new_name = base + '.pdf' if ext.lower() != '.pdf' else name
    #                 dst_path = os.path.join(self.download_folder, new_name)
    #                 if os.path.isfile(src_path):
    #                     shutil.move(src_path, dst_path)
    #                     logger.info(f"Перемещён файл: {name} -> {new_name}")
    #     except Exception as e:
    #         logger.error(f"Ошибка при перемещении файлов: {e}")
    def move_all_pdfs(self, filename):
        try:
            src_dir = os.getcwd()  # текущая рабочая директория процесса [6]
            src_path = os.path.join(src_dir, filename)
            if not os.path.isfile(src_path):
                logger.error(f"Файл не найден: {src_path}")
                return False

            # Обеспечим наличие целевой директории
            dst_dir = "/app/subsidies"
            os.makedirs(dst_dir, exist_ok=True)  # безопасно многократно [18]

            base, ext = os.path.splitext(filename)
            new_name = base + ".pdf" if ext.lower() != ".pdf" else filename
            dst_path = os.path.join(dst_dir, new_name)

            shutil.move(src_path, dst_path)  # перемещаем/переименовываем [2][1]
            logger.info(f"Перемещён файл: {src_path} -> {dst_path}")
            return dst_path
        except Exception as e:
            logger.error(f"Ошибка при перемещении файла {filename} в /app/subsidies: {e}")
            self.safe_quit(driver)
            return False

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
            self.safe_quit(driver)
            status = False

        if status: 
            try:
                self.insert(driver, wait, self.code)
            except:
                logger.info(f"Текущий URL: {href}")
                driver.back()
                self.random_sleep(5,7)
                try:
                  self.insert(driver, wait, self.code)
                except:
                    self.safe_quit(driver)
                    logger.info('driver.back не помог вставке')  
            try:
                self.click_element(wait, driver, 'button.mrx-btn.btn.icon-left.minfin-search__btn.mrx-btn-lg.mrx-btn-primary.ng-star-inserted')
            except:
                logger.error('Не удалось нажать кнопку поиска.')
                status = False
                href = driver.current_url
                logger.info(f"Текущий URL: {href}")
                self.safe_quit(driver)
            
        if status:
            current_url = driver.current_url
            logger.info(f"Текущий URL: {current_url}")
            try:
                href = self.get_href(wait, "a.selection-head")
            except:
                driver.back()
                current_url = driver.current_url
                logger.info(f"Текущий URL: {current_url}")
                self.safe_quit(driver)
                


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
                    self.href = href
                    driver.get(href)
                    logger.info(f"Перешли по ссылке: {href}")
                except Exception as e:
                    logger.error(f"Ошибка при переходе по ссылке {href}: {e}")
                    status = False
                    self.safe_quit(driver)

        self.random_sleep(7, 10)
    
        try:
            self.click_element(wait, driver, 'div.competition-head-more')
            logger.info('Кликнули Подробнее')
            self.shifr[self.code] = driver.current_url
        except:
            logger.info('НЕ Кликнули Подробнее')
        self.random_sleep(5, 7)
        logger.debug(f"Текущий URL: {driver.current_url}")
        # selector = self.get_announcements_container_class(driver.current_url)
        # logger.info(f'селектор {selector}')
        try:
            self.get_treb(driver, wait)
            logger.info('Нажали на документы')
            self.random_sleep(2, 4)
        except:
            logger.info('не нажали на документы')

        self.random_sleep(2, 5)
        if status:
            href = None
            for _ in range(3):
                href = self.download_doc(wait, driver)
                self.random_sleep(1, 2)
                if href:
                    break

            if href and status:
                try:
                    filename = self.downl(href, self.code)
                except:
                    logger.info('не получили ссылку на документ')
                    self.safe_quit(driver)
                    status = False

                if filename:
                    try:
                        self.move_all_pdfs(filename)
                        logger.warning('переместили')
                    except:
                       logger.warning('не удалось переместить в /app/subsidies') 
                
            # if href and status:
            #     # сохраняем строго в /app/subsidies/<code>.pdf
            #     target_path = os.path.join(self.download_folder, f"{self.code}.pdf")  # /app/subsidies/XXXX.pdf [22]
            #     saved = self.downl(href, target_path, driver=driver)
            #     if saved:
            #         logger.warning('скачали')
            #     else:
            #         logger.warning('Не удалось получить ссылку на документ')
            #         status = False
        try:
            self.safe_quit(driver)
        except:
            logger.info('уже закрыли')
        return self.shifr


from pathlib import Path
DOWNLOADS_DIR = Path("/app/downloads")  # совпадает с точкой монтирования из compose

class Link_to_excel:
    def __init__(self, excel_path: Path = DOWNLOADS_DIR / "Реестр отборов.xlsx"):
        self.excel_path = Path(excel_path)
        abs_path = self.excel_path.resolve()
        logger.info(f"Открываем Excel по пути: {abs_path}")
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Файл не найден: {abs_path}")
        sheets = pd.read_excel(self.excel_path, sheet_name=None)
        df_all = pd.concat(sheets.values(), ignore_index=True)
        self.df = df_all
        if 'ссылка' not in self.df.columns:
            self.df['ссылка'] = pd.Series(dtype='string')  # или object
        else:
            self.df['ссылка'] = self.df['ссылка'].astype('string')  # или .astype('object') 

    def set_link(self, code, link):
        self.df.loc[self.df['Шифр отбора'] == code, 'ссылка'] = link
        logger.info(f"Установлена ссылка для кода {code}: {link}")

    def save(self, filename: Path = None):
        target = Path(filename) if filename else self.excel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(target, index=False, engine="openpyxl")
        logger.info(f"Файл сохранён: {target.resolve()}")



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
            downloader = BudgetDocDownloader(code, '/app/subsidies')
            downloader.run()
            link = downloader.href
            link_to_excel = Link_to_excel()
            link_to_excel.set_link(code, link)
            link_to_excel.save()
            logger.info(f"Обработан код: {code}, результат: {link}")
            self.channel.basic_publish(exchange='', routing_key='parser_pdf', body=code)
        except Exception as e:
            logger.error(f"Ошибка при обработке кода {code}: {e}")
            self.channel.basic_publish(exchange='', routing_key='error_codes', body=code)
            
        
        finally:
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