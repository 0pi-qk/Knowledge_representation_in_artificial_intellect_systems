import random
import re
import urllib.request
from urllib.parse import urlparse

import bs4
import requests
from nltk.tokenize import word_tokenize

from database import *

nameList = ['Александр', 'Михаил', 'Максим', 'Лев', 'Марк', 'Артем', 'Иван', 'Матвей', 'Дмитрий', 'Даниил', 'София',
            'Мария', 'Анна', 'Алиса', 'Виктория', 'Ева', 'Полина', 'Александра', 'Василиса', 'Варвара']
class Crawler:
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.connection = create_connection(self.dbFileName, "postgres", "22343056", "localhost", "5432")

    def __del__(self):
        self.connection.close()

    def initDB(self):
        execute_query(self.connection, create_wordList_table)
        execute_query(self.connection, create_URLList_table)
        execute_query(self.connection, create_linkBtwURL_table)
        execute_query(self.connection, create_wordLocation_table)
        execute_query(self.connection, create_linkWord_table)

    def addToIndex(self, soup, url):
        if self.isIndexed(url):
            return

        text = self.getTextOnly(soup)
        words = word_tokenize(text)

        urlId: int = self.getEntryId('urllist', 'url', url)  # add URL in db

        for i in range(len(words)):
            word: str = words[i].lower()

            if re.fullmatch('[.,₽&•$><*\'`?!+()\/=;:@~#{}\[\]\-]*', word) or word in nameList:
                pass
            else:
                id_wordlist = self.getEntryId('wordlist', 'word', word)  # add word in db

                cursor = self.connection.cursor()

                cursor.execute("""INSERT INTO wordLocation (fk_word_id, fk_URL_id, location) VALUES (
                               (%s), (%s), (%s));""" % (id_wordlist, urlId, i))

    def getTextOnly(self, doc):
        text: str = ""

        all_tag = list(filter(None, [tag.get_text(strip=True, separator='\n') for tag in doc.find_all()]))

        for tag in all_tag:
            text += tag.replace("'", "").replace('"', '').replace("`", '') + '\n'

        return text

    def isIndexed(self, url):  # have URL in db
        cursor = self.connection.cursor()
        cursor.execute("""SELECT EXISTS(SELECT * FROM URLlist WHERE URL = '%s');""" % (url,))
        if cursor.fetchall()[0][0]:
            cursor.execute("""SELECT EXISTS(SELECT * FROM wordLocation JOIN URLList ON 
            URLList.rowid = fk_URL_id WHERE URL = '%s');""" % (url,))
            if cursor.fetchall()[0][0]:
                return True
        return False

    def addLinkRef(self, urlFrom, urlTo):  # add link between URL
        cursor = self.connection.cursor()
        cursor.execute("""SELECT rowid FROM URLList WHERE URL = '%s';""" % (urlFrom,))
        tmp1 = cursor.fetchall()[0][0]
        cursor.execute("""SELECT rowid FROM URLList WHERE URL = '%s';""" % (urlTo,))
        tmp2 = cursor.fetchall()[0][0]
        cursor.execute("""INSERT INTO linkBtwURL (fk_FromURL_id, fk_ToURL_id) 
                       VALUES ((%s), (%s));""" % (tmp1, tmp2))

    def stat(self, table):
        cursor = self.connection.cursor()
        print('\n\nРезультаты парсинга:\n')
        print(f'Данные для построения графика: {table}\n')
        print('20 наиболее частых слов:')
        cursor.execute("""SELECT fk_word_id FROM wordlocation;""")
        wordlist = cursor.fetchall()
        top_word = []
        for word in list(set(wordlist)):
            cursor.execute("""SELECT word FROM wordlist WHERE rowid = '%s';""" % (word[0],))
            top_word += [(cursor.fetchall()[0][0], wordlist.count(word))]
        for i in range(0, 20):
            try:
                print(sorted(top_word, key=lambda word: word[1], reverse=True)[i][0], ' - ',
                      sorted(top_word, key=lambda word: word[1], reverse=True)[i][1])
            except:
                continue

        print('\n20 наиболее частых доменов:')
        cursor.execute("""SELECT url FROM URLList;""")
        domens = []
        for url in cursor.fetchall():
            domens += [urlparse(url[0]).netloc]
        top_domen = []
        for domen in list(set(domens)):
            top_domen += [(domen, domens.count(domen))]
        for i in range(0, 20):
            try:
                print(sorted(top_domen, key=lambda domen: domen[1], reverse=True)[i][0], ' - ',
                      sorted(top_domen, key=lambda domen: domen[1], reverse=True)[i][1])
            except:
                continue
        print('\n')

    def crawl(self, urlList, maxDepth):
        newPageSet = set()
        table = []

        for currDepth in range(0, maxDepth):
            print('\nТекущий уровень - ', currDepth)

            for url in urlList:

                cursor = self.connection.cursor()
                urlFrom: int = self.getEntryId('urllist', 'url', url)
                cursor.execute("""SELECT COUNT(rowid) FROM linkBtwURL WHERE fk_FromURL_id = '%s';""" % (urlFrom,))

                if cursor.fetchall()[0][0]:
                    print('Страница уже была обработана!')
                    cursor.execute("""SELECT fk_ToURL_id FROM linkBtwURL WHERE fk_FromURL_id = '%s';""" % (urlFrom,))

                    for id in cursor.fetchall():
                        cursor.execute(
                            """SELECT url FROM urllist WHERE rowid = '%s';""" % (id[0],))
                        newPageSet.add(cursor.fetchall()[0][0])

                    continue

                try:
                    response = urllib.request.urlopen(url)
                    status_code = response.getcode()
                    if status_code == 200:
                        print(f"Статус код - {status_code}. Страница открыта ( {url} )")
                    else:
                        print(f"Страница вернула код состояния {status_code}")
                except urllib.error.HTTPError as e:
                    print(f"Ошибка HTTP: {e.code} - {e.reason}")
                    continue
                except urllib.error.URLError as e:
                    print(f"Ошибка URL: {e.reason}")
                    continue
                except:
                    print('Неизвестная ошибка')

                html_doc = requests.get(url).text
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                self.addToIndex(soup, url)

                print(url)

                for link in soup.findAll('a'):

                    if link.get('href') is None or link.get('href') == '' or 'javascript' in link.get('href')\
                            or "'" in link.get('href') or '"' in link.get('href'):
                        continue

                    domen = 'https://' + urlparse(url).netloc

                    if link.get('href') == '#':
                        print(domen)
                        self.getEntryId('urllist', 'url', domen)
                        self.addLinkRef(url, domen)
                        newPageSet.add(domen)
                    elif link.get('href')[0] == '/':
                        print(domen + link.get('href'))
                        self.getEntryId('urllist', 'url', domen + link.get('href'))
                        newPageSet.add(domen + link.get('href'))
                    elif link.get('href')[0] != 'h':
                        print(domen + '/' + link.get('href'))
                        self.getEntryId('urllist', 'url', domen + '/' + link.get('href'))
                        self.addLinkRef(url, domen + '/' + link.get('href'))
                        newPageSet.add(domen + '/' + link.get('href'))
                    else:
                        print(link.get('href'))
                        self.getEntryId('urllist', 'url', link.get('href'))
                        self.addLinkRef(url, link.get('href'))
                        newPageSet.add(link.get('href'))

                cursor = self.connection.cursor()
                cursor.execute("""SELECT COUNT(rowid) FROM wordlist;""")
                count_wordlist = cursor.fetchall()[0][0]
                cursor.execute("""SELECT COUNT(rowid) FROM linkBtwURL;""")
                count_linkBtwURL = cursor.fetchall()[0][0]

                table += [(currDepth, count_wordlist, count_linkBtwURL)]

            self.connection.commit()
            urlList = list(random.sample(list(newPageSet), int(len(newPageSet) * 0.05 + 1)))
            print(f"Страниц - {len(newPageSet)}. Из них 5% - {int(len(newPageSet) * 0.05 + 1)}")

        self.stat(table)

    def getEntryId(self, tableName, fieldName, value):

        cursor = self.connection.cursor()

        cursor.execute("""SELECT count (*) FROM %s WHERE %s = '%s';""" % (tableName, fieldName, value))

        if cursor.fetchall()[0][0] == 0:
            cursor.execute("""INSERT INTO %s (%s) VALUES ('%s') RETURNING rowid;""" % (tableName, fieldName, value))
            return cursor.fetchall()[0][0]
        else:  # have URL in db
            cursor.execute("""SELECT rowid FROM %s WHERE %s = '%s';""" % (tableName, fieldName, value))
            return cursor.fetchall()[0][0]
