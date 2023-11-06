from Crawler import *
# import nltk #При первом запуске

if __name__ == '__main__':
    main = Crawler("pzvsii2")
    main.initDB()

    #main.crawl(["https://habr.com/ru/companies/first/articles/769736/",
    #            "https://cyberleninka.ru/article/n/zadacha-o-volnah-maloy-amplitudy-v-kanale-peremennoy-glubiny"], 2)

    main.crawl(["https://aftershock.news/?q=node/1304253"], 5)
