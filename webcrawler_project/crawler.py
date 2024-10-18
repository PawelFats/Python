import requests
from bs4 import BeautifulSoup
import re
import sqlite3
from urllib.parse import urlparse

class WebCrawler:
    def __init__(self, urls, db_name='webcrawler.db'):
        self.urls = urls
        self.visited_urls = set()
        self.url_count = 0
        self.word_count = 0
        self.db_name = db_name
        self.social_networks = ['facebook.com', 'twitter.com', 'instagram.com', 'vk.com', 'ok.ru', 'linkedin.com']

        # Подключение к БД и создание таблиц
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """Создаем таблицы в БД для хранения данных."""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS urllist (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               url TEXT UNIQUE)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS wordlist (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               word TEXT UNIQUE,
                               is_filtered INTEGER DEFAULT 0)''')  # Добавляем признак фильтрации
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS wordlocation (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               url_id INTEGER,
                               word_id INTEGER,
                               location INTEGER,
                               FOREIGN KEY (url_id) REFERENCES urllist(id),
                               FOREIGN KEY (word_id) REFERENCES wordlist(id))''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS linkBetweenURL (
                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                               from_id INTEGER,
                               to_id INTEGER,
                               FOREIGN KEY (from_id) REFERENCES urllist(id),
                               FOREIGN KEY (to_id) REFERENCES urllist(id))''')
        self.conn.commit()

    def crawl(self, max_depth=2):
        """Основной процесс обхода страниц с максимальной глубиной max_depth."""
        for depth in range(max_depth):
            new_urls = []
            for url in self.urls:
                if self.url_count >= 10:
                    break
                if url not in self.visited_urls:
                    self.visited_urls.add(url)
                    self.process_url(url, depth)
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
            # Устанавливаем правильную кодировку, если она не указана сервером
            if response.encoding is None:
                response.encoding = response.apparent_encoding
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                print(f"Обработка {url} на глубине {depth}")
                self.add_to_index(soup, url)
                self.url_count += 1
        except Exception as e:
            print(f"Ошибка при обработке {url}: {e}")

    def add_to_index(self, soup, url):
        """Добавление содержимого страницы и ссылок в базу данных."""
        # Получаем текст страницы
        text = self.get_text_only(soup)
        words = self.separate_words(text)

        # Добавляем URL в таблицу
        self.cursor.execute("INSERT OR IGNORE INTO urllist (url) VALUES (?)", (url,))
        self.cursor.execute("SELECT id FROM urllist WHERE url = ?", (url,))
        url_id = self.cursor.fetchone()[0]

        # Индексируем слова
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
        self.conn.commit()

    def get_text_only(self, soup):
        """Извлекаем только текст страницы, без HTML-тегов."""
        return soup.get_text()

    def separate_words(self, text):
        """Разбиваем текст на отдельные слова."""
        words = re.findall(r'\b[а-яА-Яa-zA-Z]+\b', text)  # Только буквы (латиница и кириллица)
        return words

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
                        self.add_link(url, full_url)
                return links
        except Exception as e:
            print(f"Ошибка при извлечении ссылок с {url}: {e}")
        return []

    def add_link(self, from_url, to_url):
        """Добавление ссылки в таблицу linkBetweenURL."""
        # Получаем или добавляем from_url в urllist
        self.cursor.execute("INSERT OR IGNORE INTO urllist (url) VALUES (?)", (from_url,))
        self.cursor.execute("SELECT id FROM urllist WHERE url = ?", (from_url,))
        from_id = self.cursor.fetchone()[0]

        # Получаем или добавляем to_url в urllist
        self.cursor.execute("INSERT OR IGNORE INTO urllist (url) VALUES (?)", (to_url,))
        self.cursor.execute("SELECT id FROM urllist WHERE url = ?", (to_url,))
        to_id = self.cursor.fetchone()[0]

        # Добавляем запись о ссылке между двумя URL
        self.cursor.execute("INSERT INTO linkBetweenURL (from_id, to_id) VALUES (?, ?)", (from_id, to_id))
        self.conn.commit()

    def analyze_db(self):
        """Анализ содержимого таблиц БД."""
        print("Анализ таблиц БД:")

        # Количество записей в таблицах
        self.cursor.execute("SELECT COUNT(*) FROM urllist")
        print(f"Количество URL в таблице urllist: {self.cursor.fetchone()[0]}")
        self.cursor.execute("SELECT COUNT(*) FROM wordlist")
        print(f"Количество уникальных слов в таблице wordlist: {self.cursor.fetchone()[0]}")
        self.cursor.execute("SELECT COUNT(*) FROM wordlocation")
        print(f"Количество записей в таблице wordlocation: {self.cursor.fetchone()[0]}")
        self.cursor.execute("SELECT COUNT(*) FROM linkBetweenURL")
        print(f"Количество записей в таблице linkBetweenURL: {self.cursor.fetchone()[0]}")

        # 20 наиболее часто проиндексированных доменов
        print("\n20 наиболее часто проиндексированных доменов:")
        self.cursor.execute('''SELECT SUBSTR(url, INSTR(url, '//') + 2, INSTR(SUBSTR(url, INSTR(url, '//') + 2), '/') - 1) AS domain, COUNT(*) as freq
                               FROM urllist
                               GROUP BY domain
                               ORDER BY freq DESC
                               LIMIT 20''')
        for row in self.cursor.fetchall():
            print(row)

        # 20 наиболее часто встречающихся слов
        print("\n20 наиболее часто встречающихся слов:")
        self.cursor.execute('''SELECT word, COUNT(wordlocation.word_id) as freq
                               FROM wordlist
                               JOIN wordlocation ON wordlist.id = wordlocation.word_id
                               GROUP BY word
                               ORDER BY freq DESC
                               LIMIT 20''')
        for row in self.cursor.fetchall():
            print(row)

    def __del__(self):
        self.conn.close()

if __name__ == '__main__':
    # Список URL для парсинга
    urls = ['https://ngs.ru/', 'https://www.gazeta.ru/']
    crawler = WebCrawler(urls)
    crawler.crawl()
    crawler.analyze_db()
