import requests
from bs4 import BeautifulSoup
import re
import sqlite3
from urllib.parse import urlparse
import matplotlib.pyplot as plt

class WebCrawler:
    def __init__(self, urls, db_name='webcrawler.db'):
        self.urls = urls
        self.visited_urls = set()
        self.url_count = 0
        self.word_count = 0
        self.db_name = db_name
        self.social_networks = ['facebook.com', 'twitter.com', 'instagram.com', 'vk.com', 'ok.ru', 'linkedin.com', 't.me', 'telegram.org']

        # Подключение к БД и создание таблиц
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """Создаем таблицы в БД для хранения данных."""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS urllist (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            url TEXT,
                            domain TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS wordlist (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            word TEXT UNIQUE,
                            is_filtered INTEGER DEFAULT 0)''')  # Убрали комментарий
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

    def clear_db(self):
        """Очищает все данные в базе перед запуском краулера."""
        print("Очистка базы данных...")
        self.cursor.execute('DELETE FROM urllist')
        self.cursor.execute('DELETE FROM wordlist')
        self.cursor.execute('DELETE FROM wordlocation')
        self.cursor.execute('DELETE FROM linkBetweenURL')
        self.conn.commit()
        print("База данных очищена.")

    def isIndexed(self, url) -> bool:
        query = "SELECT EXISTS(SELECT 1 FROM urllist WHERE url = ?)"
        self.cursor.execute(query, (url,))
        result = self.cursor.fetchone()
        return result[0] == 1

    def crawl(self, max_depth=2):
        """Основной процесс обхода страниц с максимальной глубиной max_depth."""
        print("Начало краулинга...")
        for depth in range(max_depth):
            new_urls = []
            for url in self.urls:
                if self.url_count >= 20:
                    break
                if not self.isIndexed(url):
                    print(f"Переход на страницу: {url} (глубина: {depth})")
                    self.process_url(url, depth)
                else:
                    print(f"Страница {url} уже была проиндексирована.")

                # Извлекаем все ссылки с текущей страницы для следующей глубины
                new_urls.extend(self.get_links_from_page(url))
            self.urls = new_urls
        print("Краулинг завершен.")

    def process_url(self, url):
        if any(social in url for social in self.social_networks):
            print(f"Пропуск страницы социальной сети: {url}")
            return
        try:
            response = requests.get(url)
            response.encoding = response.apparent_encoding
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                self.add_to_index(soup, url)
                self.url_count += 1
            else:
                print(f"Ошибка загрузки страницы {url}: Статус {response.status_code}")
        except Exception as e:
            print(f"Ошибка при обработке {url}: {e}")

    def add_to_index(self, soup, url):
        """Добавление содержимого страницы и ссылок в базу данных."""
        # Проверяем, был ли URL уже проиндексирован
        if self.isIndexed(url):
            print(f"URL {url} уже проиндексирован.")
            return
        
        # Получаем текст страницы
        text = self.get_text_only(soup)
        words = self.separate_words(text)

        # Извлекаем домен из URL
        domain = urlparse(url).netloc
        
        # Добавляем URL и домен в таблицу urllist
        self.cursor.execute("INSERT OR IGNORE INTO urllist (url, domain) VALUES (?, ?)", (url, domain))
        self.cursor.execute("SELECT id FROM urllist WHERE url = ?", (url,))
        url_id = self.cursor.fetchone()[0]

        # Индексируем слова
        for idx, word in enumerate(words):
            word = word.lower()
            if word in ["и", "а", "но", "за"] or word.isdigit():  # Простая фильтрация слов
                continue

            # Добавляем слово в таблицу wordlist
            self.cursor.execute("INSERT OR IGNORE INTO wordlist (word) VALUES (?)", (word,))
            self.cursor.execute("SELECT id FROM wordlist WHERE word = ?", (word,))
            word_id = self.cursor.fetchone()[0]

            # Добавляем местоположение слова
            self.cursor.execute("INSERT INTO wordlocation (url_id, word_id, location) VALUES (?, ?, ?)",
                                (url_id, word_id, idx))
        self.conn.commit()

    def get_text_only(self, soup, flag=False):
        """Извлекаем только текст страницы, без HTML-тегов. Для Википедии удаляем служебные ссылки."""
        
        # Если это страница Википедии, удаляем служебные ссылки
        if flag:
            # Удаляем ссылки на редактирование (например, [править] на Википедии)
            for edit_link in soup.find_all("span", {"class": "mw-editsection"}):
                edit_link.decompose()

            # Удаляем навигационные блоки (часто бывают в Википедии, например, меню навигации)
            for nav_box in soup.find_all("div", {"class": "navbox"}):
                nav_box.decompose()
        
            # Удаляем ссылки на сноски или другие служебные ссылки
            for sup_link in soup.find_all("sup", {"class": "reference"}):
                sup_link.decompose()

        # Извлекаем текст страницы
        text = soup.get_text()

        # Удаляем лишние переносы на новую строку и пробелы
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def separate_words(self, text):
        """Разделяет текст на слова с использованием определённых разделителей и удаляет ссылки в квадратных скобках."""
        
        # Удаляем все ссылки или элементы в квадратных скобках, например, [1]
        text = re.sub(r'\[.*?\]', '', text)

        # Разбиваем текст на слова, используя перечисленные разделители
        words = re.split(r'[,\.;:@#?!&$()\[\]—\s]+', text)

        # Убираем пустые строки, которые могут возникнуть после разбиения
        words = [word for word in words if word]

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
                return links
        except Exception as e:
            print(f"Ошибка при извлечении ссылок с {url}: {e}")
        return []

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

    def close(self):
        """Закрывает соединение с БД."""
        self.conn.close()

if __name__ == '__main__':
    urls = ['https://ngs.ru/']
    crawler = WebCrawler(urls)
    crawler.clear_db()
    crawler.crawl()
    crawler.analyze_db()
    crawler.close()
