from Searcher import *
# import nltk #При первом запуске


def menu():
    while True:
        print(
            '\tMenu:\n'
            '1|  Crawler\n'
            '2|  Searcher'
        )
        n = int(input('--> '))
        if n == 1:
            DB_name = input('\nEnter database mame - ')
            URL = list(input('Enter URL (format: url_1 url_2) - ').split())
            depth = int(input('Enter depth - '))

            main = Crawler(DB_name)
            main.initDB()
            main.crawl(URL, depth)
        elif n == 2:
            DB_name = input('\nEnter database mame - ')
            mySearcher = Seacher(DB_name)

            mySearchQuery = input('\nEnter search query - ')
            mySearcher.getSortedList(mySearchQuery)
        print('\n\n')


if __name__ == '__main__':
    menu()
