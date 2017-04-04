#-*-coding:utf8-*-
import pymongo
from format_iaga02_03 import *

connection = pymongo.MongoClient()

tdb = connection.Demo
post_info = tdb.test

#文件路径名，默认当前路径
filename = "kou20170319vmin.min"

god = readIAGA(filename)

post_info.insert(god)

print 'Well Done!'