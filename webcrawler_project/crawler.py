import requests
from bs4 import BeautifulSoup
import time
import re
import sqlite3

class WebCrawler:
    def __init__(self, urls, db_name='webcrawler.db'):
        self.urls = urls
        self.visited_urls = set()
        self.url_count = 0
        self.word_count = 0
        self.db_name = db_name
        self.max_depth = 2  # Максимальная глубина обхода
        self.social_networks = ['facebook.com', 'twitter.com', 'instagram.com', 'vk.com', 'ok.ru', 'linkedin.com']

        # Подключение к БД и создание таблиц
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """Создаем таблицы в БД для хранения данных."""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS urllist (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               url TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS wordlist (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               word TEXT UNIQUE)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS wordlocation (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               url_id INTEGER,
                               word_id INTEGER,
                               location INTEGER,
                               FOREIGN KEY (url_id) REFERENCES urllist(id),
                               FOREIGN KEY (word_id) REFERENCES wordlist(id))''')
        self.conn.commit()

    def crawl(self):
        """Основной процесс обхода страниц"""
        for depth in range(self.max_depth):
            new_urls = []
            for url in self.urls:
                if self.url_count >= 100:
                    break
                if url not in self.visited_urls:
                    self.visited_urls.add(url)
                    self.process_url(url, depth)
                    time.sleep(2)  # 2 секунды на обработку страницы

                # Извлекаем все ссылки с текущей страницы для следующей глубины
                new_urls.extend(self.get_links_from_page(url))
            self.urls = new_urls

    def process_url(self, url, depth):
        """Обрабатываем URL: загружаем страницу и фильтруем социальные сети."""
        if any(social in url for social in self.social_networks):
            print(f"Пропуск страницы социальной сети: {url}")
            return
        try:
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                print(f"Обработка {url} на глубине {depth}")
                self.extract_words(soup, url)
                self.url_count += 1
        except Exception as e:
            print(f"Ошибка при обработке {url}: {e}")

    def extract_words(self, soup, url):
        """Извлекаем текст и разбиваем на слова."""
        text = soup.get_text()
        words = re.findall(r'\b\w+\b', text)
        self.update_word_list(words, url)

    def update_word_list(self, words, url):
        """Обновляем таблицы с URL, словами и местоположениями."""
        self.cursor.execute("INSERT INTO urllist (url) VALUES (?)", (url,))
        url_id = self.cursor.lastrowid

        for idx, word in enumerate(words):
            word = word.lower()
            if word in ["и", "а", "но", "за"] or word.isdigit():
                continue

            # Добавляем слово в таблицу wordlist
            self.cursor.execute("INSERT OR IGNORE INTO wordlist (word) VALUES (?)", (word,))
            self.cursor.execute("SELECT id FROM wordlist WHERE word = ?", (word,))
            word_id = self.cursor.fetchone()[0]

            # Добавляем местоположение слова
            self.cursor.execute("INSERT INTO wordlocation (url_id, word_id, location) VALUES (?, ?, ?)",
                                (url_id, word_id, idx))

            self.word_count += 1
            if self.word_count >= 300000:
                break
        self.conn.commit()

    def get_links_from_page(self, url):
        """Получаем все ссылки со страницы."""
        try:
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = []
                for link in soup.find_all('a', href=True):
                    full_url = link['href']
                    if full_url.startswith('http'):
                        links.append(full_url)
                return links
        except Exception as e:
            print(f"Ошибка при извлечении ссылок с {url}: {e}")
        return []

    def report(self):
        """Отчет о количестве URL и слов."""
        print(f"Проиндексировано URL: {self.url_count}")
        self.cursor.execute("SELECT COUNT(*) FROM wordlist")
        print(f"Всего слов: {self.cursor.fetchone()[0]}")
        self.cursor.execute("SELECT COUNT(*) FROM wordlocation")
        print(f"Места слов: {self.cursor.fetchone()[0]}")

    def __del__(self):
        self.conn.close()

if __name__ == '__main__':
    # Список URL для парсинга
    urls = ['https://ngs.ru/', 'https://www.gazeta.ru/']
    crawler = WebCrawler(urls)
    crawler.crawl()
    crawler.report()
