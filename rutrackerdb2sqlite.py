#!/usr/bin/python3

#Парсер открытой базы раздач рутракера в базу данных SQLite3
#Раздача: http://rutracker.org/forum/viewtopic.php?t=4824458    магнет:   magnet:?xt=urn:btih:4FAB4F7F42981E51ED9463695C27D39772AFDCD4&tr=http%3A%2F%2Fbt2.rutracker.cc%2Fann%3Fmagnet
#
#Автор: SL_RU
#Почта: sl_ru@live.com
#VK: https://vk.com/sl_ru_dev


#Запуск: "python3 rutrackerdb2sqlite.py"


#В файле .db создаётся две таблицы:
#categories  - категории. Столбцы: ID, NAME (имя категории) и FILE(файл категории. не нужно)

#data        - сами раздачи. Столбцы: ID (ID раздачи), CAT_ID(ID категории), FORUM_ID(ID форума), FORUM_NAME, 
#HASH (хэш раздачи(или уже готовая магнет ссылка, если установили CREATE_MAGNET = True)),
#NAME (имя раздачи), SIZE(Размер раздачи в байтах), REG_DATE(дата регистрации)




#ТУТ ИЗМЕНИТЕ ПУТЬ К БАЗЕ НА СВОЙ!
dbpath = "/mnt/doc/torrents/rutracker-torrents/20151030"
#ЕСЛИ True, то вместо хэша в поле HASH будет магнет ссылка, иначе - хэш
CREATE_MAGNET = True 
#Файл sqlite базы
sqlDBPath = "rutrackerMAG.db"






import sqlite3 as lite
import os

db = lite.connect(sqlDBPath) #Путь к файлу sqllite3

db.execute("CREATE TABLE categories (ID INT PRIMARY KEY NOT NULL, NAME TEXT NOT NULL, FILE TEXT NOT NULL);")
db.execute("CREATE TABLE data (ID INT PRIMARY KEY NOT NULL, CAT_ID INT NOT NULL, FORUM_ID INT NOT NULL, FORUM_NAME TEXT NOT NULL, "+
	"HASH TEXT NOT NULL, NAME TEXT NOT NULL, SIZE INT NOT NULL, REG_DATE TEXT NOT NULL);")
db.commit()

files = list()

with open(os.path.join(dbpath, "category_info.csv")) as f:
	for l in f:
		q = l.split(";")
		q[0] = q[0].replace("\"", "")
		q[1] = q[1].replace("\"", "'")
		files.append((q[2][:-1].replace("\"", ""), q[0]))
		q[2] = q[2][:-1].replace("\"", "'")
		db.execute("INSERT INTO categories (ID, NAME, FILE) VALUES (" + q[0] + ", " + q[1] + ", " + q[2] + ");")
db.commit()

for i in files:
	print("PARSING: " + i[0])
	with open(os.path.join(dbpath, i[0])) as f:
		for l in f:
			q = l.split("\";\"")
			q[0] = q[0].replace("\"", "")
			q[1] = "\"" + q[1] + "\""
			q[2] = q[2]
			if CREATE_MAGNET:
				q[3] = "\"magnet:?xt=urn:btih:" + q[3] + "&tr=http://bt2.rutracker.cc/ann?magnet\""
			else:
				q[3] = "\"" + q[3] + "\""
			q[4] = "\"" + q[4] + "\""
			q[5] = q[5].replace("\"", "")
			q[6] = "\"" + q[6][:-1]
			q.append(i[1])
			com = "INSERT INTO data (FORUM_ID, FORUM_NAME, ID, HASH, NAME, SIZE, REG_DATE, CAT_ID) VALUES ("
			for z in q:
				com += z + ", "
			com = com[:-2] + ");"
			db.execute(com)
	db.commit()
