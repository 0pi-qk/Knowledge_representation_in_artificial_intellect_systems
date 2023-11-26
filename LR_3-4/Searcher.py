from Crawler import *


class Searcher:
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.connection = create_connection(self.dbFileName, "postgres", "22343056", "localhost", "5432")

    # 0. Деструктор
    def __del__(self):
        self.connection.close()

    def getWordsIds(self, queryString):
        rowidList = list()

        for word in queryString:
            cursor = self.connection.cursor()
            cursor.execute("""SELECT rowid FROM wordList WHERE word = '{}' LIMIT 1; """.format(word))

            result_row = cursor.fetchone()

            if result_row is not None:
                word_rowid = result_row[0]

                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                raise Exception("Одно из слов поискового запроса не найдено:" + word)

        return rowidList

    def getMatchRows(self, queryString):

        wordsList = queryString.split(' ')

        wordsidList = self.getWordsIds(wordsList)

        sqlFullQuery = """"""

        sqlpart_Name = list()
        sqlpart_Join = list()
        sqlpart_Condition = list()

        for wordIndex in range(0, len(wordsList)):

            wordID = wordsidList[wordIndex]

            if wordIndex == 0:

                sqlpart_Name.append("""w0.fk_url_id""")
                sqlpart_Name.append(""", w0.location""")

                sqlpart_Condition.append("""WHERE w0.fk_word_id={}""".format(wordID))

            elif len(wordsList) >= 2:

                sqlpart_Name.append(""", w{}.location""".format(wordIndex))

                sqlpart_Join.append("""JOIN wordlocation w{} ON w0.fk_url_id=w{}.fk_url_id """.format(wordIndex,
                                                                                                      wordIndex,
                                                                                                      wordIndex))

                sqlpart_Condition.append(""" AND w{}.fk_word_id={}""".format(wordIndex, wordID))

        sqlFullQuery += "SELECT "

        for sqlpart in sqlpart_Name:
            sqlFullQuery += sqlpart

        sqlFullQuery += " FROM wordlocation w0 "

        for sqlpart in sqlpart_Join:
            sqlFullQuery += sqlpart

        for sqlpart in sqlpart_Condition:
            sqlFullQuery += sqlpart

        sqlFullQuery += ";"

        cursor = self.connection.cursor()
        cursor.execute(sqlFullQuery)
        cur = cursor.fetchall()

        rows = [row for row in cur]

        return rows, wordsidList

    def normalizeScores(self, scores, smallIsBetter=0):
        resultDict = dict()

        vsmall = 0.00001
        minscore = min(scores.values())
        maxscore = max(scores.values())

        for (key, val) in scores.items():

            if smallIsBetter:
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                resultDict[key] = float(val) / maxscore

        return resultDict

    def locationScore(self, rowsLoc):

        locationsDict = dict()

        for row in rowsLoc:
            locationsDict[row[0]] = 1000000

        for row in rowsLoc:
            sum = 0

            for id in range(1, len(row)):
                sum += row[id]

            urlId = row[0]

            if locationsDict[urlId] > sum:
                locationsDict[urlId] = sum

        return self.normalizeScores(locationsDict, smallIsBetter=1)

    def geturlname(self, id):

        cursor = self.connection.cursor()
        cursor.execute("""SELECT url FROM urllist WHERE rowid = {}""".format(id))

        return cursor.fetchone()[0]

    def getSortedList(self, queryString): # допилить до формата
        queryString = queryString.lower()
        rowsLoc, _ = self.getMatchRows(queryString)

        m1Scores = self.locationScore(rowsLoc)
        m2Scores = self.pagerankScore(rowsLoc)

        rankedScoresList = list()

        id_l = list(m1Scores.keys())
        m1 = list(m1Scores.values())
        m2 = list(m2Scores.values())

        for i in range(0, len(m1Scores)):
            rankedScoresList.append([id_l[i], m1[i], m2[i], (m1[i] + m2[i]) / 2, self.geturlname(id_l[i])])

        def sort(score):
            return score[3]

        rankedScoresList.sort(reverse=True, key=sort)

        print("\n  IdURL  |    m1    |    m2    |    m3    |   URL")
        print("-------------------------------------------------------")
        for (id, m1, m2, m3, url) in rankedScoresList[0:10]:
            print(" {:>5}   |   {:.2f}   |   {:.2f}   |   {:.2f}   |   {}".format(id, m1, m2, m3, url))
        print('\n\n')

        text = ""

        for (_, _, _, _, url) in rankedScoresList[0:3]:
            html_doc = requests.get(url).text
            soup = bs4.BeautifulSoup(html_doc, "html.parser")
            text += Crawler(self.dbFileName).getTextOnly(soup) + "\nНОВАЯ СТРАНИЦА\n"

        ql = queryString.split(' ')
        for i in range(0, len(ql)):
            text = text.lower().replace(ql[i], "\033[4{}m{}\033[0m".format(i + 1, ql[i]))

        print(text)

    def calculatePageRank(self, iterations=5):
        cursor = self.connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS pagerank')
        cursor.execute("""CREATE TABLE  IF NOT EXISTS  pagerank(
                            rowid SERIAL NOT NULL PRIMARY KEY,
                            urlid INTEGER,
                            score REAL
                        );""")

        cursor.execute("""DROP INDEX   IF EXISTS     wordidx;""")
        cursor.execute("""DROP INDEX   IF EXISTS     urlidx;""")
        cursor.execute("""DROP INDEX   IF EXISTS     wordurlidx;""")
        cursor.execute("""DROP INDEX   IF EXISTS     urltoidx;""")
        cursor.execute("""DROP INDEX   IF EXISTS     urlfromidx;""")
        cursor.execute("""DROP INDEX   IF EXISTS     rankurlididx;""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS wordidx       ON wordlist(word)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS urlidx        ON urllist(url)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS wordurlidx    ON wordlocation(fk_word_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS urltoidx      ON linkbtwurl(fk_tourl_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS urlfromidx    ON linkbtwurl(fk_fromurl_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS rankurlididx  ON pagerank(urlid)""")
        cursor.execute("""REINDEX INDEX wordidx;""")
        cursor.execute("""REINDEX INDEX urlidx;""")
        cursor.execute("""REINDEX INDEX wordurlidx;""")
        cursor.execute("""REINDEX INDEX urltoidx;""")
        cursor.execute("""REINDEX INDEX urlfromidx;""")
        cursor.execute("""REINDEX INDEX rankurlididx;""")

        cursor.execute("""INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist""")
        self.connection.commit()

        for i in range(iterations):
            print('itr - ', i)
            cursor.execute("""SELECT rowid FROM urllist""")
            idList = cursor.fetchall()
            for id in idList:
                pr = 0

                cursor.execute("""SELECT DISTINCT fk_fromurl_id FROM linkbtwurl WHERE fk_tourl_id = {}""".format(id[0]))
                idListFrom = cursor.fetchall()

                for idd in idListFrom:
                    cursor.execute("""SELECT score FROM pagerank WHERE urlid = {}""".format(idd[0]))
                    linkingpr = cursor.fetchone()[0]

                    cursor.execute("""SELECT count(*) FROM linkbtwurl WHERE fk_fromurl_id = {}""".format(idd[0]))
                    linkingcount = cursor.fetchone()[0]

                    pr += linkingpr/linkingcount

                pr = 0.15 + 0.85 * pr

                cursor.execute("""UPDATE pagerank SET score = {} WHERE urlid = {}""".format(pr, id[0]))
                self.connection.commit()

    def pagerankScore(self, rows):
        pagerankDict = dict()

        for row in rows:
            cursor = self.connection.cursor()

            cursor.execute("""SELECT score FROM pagerank WHERE urlid = {}""".format(row[0]))
            score = cursor.fetchone()[0]

            if score is not None:
                pagerankDict[row[0]] = score

        return self.normalizeScores(pagerankDict, smallIsBetter=0)
