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
from loguru import logger
import undetected_chromedriver as uc


class BudgetDocDownloader:
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:79.0) Gecko/20100101 Firefox/79.0'
    ]

    def __init__(self):
        self.logger = logger
        self.logger.add("logs/parser_{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days", compression="zip")
        self.shifr = {}
        self.download_folder = "downloaded_files"

    def get_random_user_agent(self):
        return random.choice(self.USER_AGENTS)

    def random_sleep(self, a=1, b=3):
        time.sleep(random.uniform(a, b))

    def init_driver(self):
        options = uc.ChromeOptions()
        options.add_argument(f'user-agent={self.get_random_user_agent()}')
        driver = uc.Chrome(options=options, browser_executable_path="/usr/bin/chromium-browser")
        wait = WebDriverWait(driver, 10)
        return driver, wait

    def insert(self, driver, key, wait):
        try:
            input_element = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "minfin-search__input"))
            )
            input_element.clear()
            input_element.send_keys(key)
            self.logger.info('шифр вставили')
            return True
        except Exception as e:
            self.logger.info(f'Ошибка вставки в серч шифра {key}: {e}', exc_info=True)
            return False

    def click_element(self, wait, driver, css_selector):
        try:
            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".overlay-class")))
        except TimeoutException:
            self.logger.info(f'Проблемы с overlay, селектор: {css_selector}')
            return False
        try:
            element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            element.click()
            return True
        except (TimeoutException, WebDriverException):
            try:
                driver.execute_script("arguments.click();", element)
                return True
            except Exception:
                self.logger.error(f"Клик через JS тоже не удался: {css_selector}", exc_info=True)
                return False

    def get_href(self, wait, selector):
        try:
            link_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
            href = link_element.get_attribute('href')
            self.logger.info('ссылка успешно получена')
            return href
        except Exception as e:
            self.logger.error(f'Ошибка при получении селектора {selector}: {e}', exc_info=True)
            return False

    def get_treb(self, driver, wait):
        try:
            div_treb = wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.competition-menu__item'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", div_treb[-1])
            div_treb[-1].click()
            self.logger.info('нажали на объявления и протоколы')
            return True
        except Exception:
            try:
                driver.execute_script("arguments.click();", div_treb[-1])
            except Exception:
                self.logger.error('не нажимается на объявления и протоколы', exc_info=True)
            return False

    def download_doc(self, wait, driver):
        try:
            link_elements = wait.until(
                EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), 'Объявление об отборе')]"))
            )
            for link_element in link_elements:
                text = link_element.text
                if re.search(r'^Объявление об отборе.*', text):
                    return link_element.get_attribute('href')
        except Exception as e:
            self.logger.error(f"не перешли на ссылку документов: {e}", exc_info=True)
        return None

    def downl(self, url, filename):
        headers = {'User-Agent': self.get_random_user_agent()}
        response = requests.get(url, stream=True, headers=headers)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            self.logger.info(f"Файл успешно скачан: {filename}")
            return filename
        else:
            self.logger.error(f"Ошибка при скачивании файла: статус {response.status_code}")
            return False

    def move_all_pdfs(self, source_dir, filename):
        os.makedirs(self.download_folder, exist_ok=True)
        for name in os.listdir(source_dir):
            if name.lower() == filename.lower():
                src_path = os.path.join(source_dir, name)
                base, ext = os.path.splitext(name)
                new_name = base + '.pdf' if ext.lower() != '.pdf' else name
                dst_path = os.path.join(self.download_folder, new_name)
                if os.path.isfile(src_path):
                    shutil.move(src_path, dst_path)
                    self.logger.info(f'Перемещён: {name} -> {new_name}')

    def safe_quit(self, driver):
        try:
            if driver:
                driver.quit()
        except Exception as e:
            self.logger.warning(f"Ошибка при закрытии драйвера: {e}", exc_info=True)

    def run(self):
        # TODO: добавить чтение кодов из Excel

        driver, wait = self.init_driver()
        status = True

        try:
            driver.get('https://promote.budget.gov.ru/public/minfin/activity')
            self.logger.info('открыли https://promote.budget.gov.ru/public/minfin/activity')
        except Exception as e:
            self.logger.info(f"не получается открыть: https://promote.budget.gov.ru/public/minfin/activity: {e}", exc_info=True)
            status = False

        self.random_sleep(10, 15)

        try:
            button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.mrx-icon.icon-table-view.icon-font-32"))
            )
            button.click()
            self.logger.info('нажали на кнопку')
        except Exception as e:
            self.logger.info(f'не нажимается на кнопку: {e}', exc_info=True)
            status = False

        self.random_sleep(20, 30)

        try:
            button1 = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.mrx-btn-label.ng-star-inserted"))
            )
            button1.click()
            self.logger.info('нажали на кнопку')
        except Exception as e:
            self.logger.info(f'не нажимается на кнопку: {e}', exc_info=True)
            status = False

        self.random_sleep(5, 10)


        try:
            button_span = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='в формате XLS']"))
     )
            print(button_span)
            # button_span.click()

            self.logger.info('нажали на кнопку')
        except Exception as e:
            self.logger.info(f'не нажимается на кнопку: {e}', exc_info=True)

        self.safe_quit(driver)
        return status


downloader = BudgetDocDownloader()
results = downloader.run()
