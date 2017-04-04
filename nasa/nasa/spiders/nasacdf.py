# -*- coding: utf-8 -*-
from scrapy.http import Request, FormRequest
from bson.objectid import ObjectId
from pymongo import MongoClient
import scrapy
import os
import sys
import urllib
import urllib2
import gridfs
import signal
import os
import json
import pika
import pymongo
import datetime
import time

class NasacdfSpider(scrapy.Spider):
    name = "nasacdf"
    allowed_domains = ["spdf.gsfc.nasa.gov"]
    start_urls = (
        'https://spdf.gsfc.nasa.gov/pub/data/',
    )
    file_path = '/home/undoingfish/Documents/'

    def report(self, count, blockSize, totalSize):
		percent = int(count*blockSize*100/totalSize)
		sys.stdout.write("\r%d%%" % percent + ' complete')
		sys.stdout.flush()

    def producer(self, file_info):
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		channel = connection.channel()

		channel.queue_declare(queue='nasacdf_task_queue', durable=True)

		message = file_info
		channel.basic_publish(exchange='',
				              routing_key='nasacdf_task_queue',
				              body=json.dumps(message),
				              properties=pika.BasicProperties(
				                 delivery_mode = 2, # make message persistent
				              ))
		print " [x] Sent %r" % (message,)
		connection.close()

    def download_file(self, download_link, final_save_path, file_name):
		print "\n第一遍写入......."
		try:
			urllib.urlretrieve(download_link, final_save_path, reporthook=self.report)
			while os.path.getsize(final_save_path)<500:
				print "\n第二遍写入......."
				urllib.urlretrieve(download_link, final_save_path, reporthook=self.report)
			sys.stdout.flush()
			sys.stdout.write("\rDownload complete, saved as %s" % (file_name) + '\n\n')
		except (urllib2.URLError, IOError) as e:
			urllib.urlretrieve(download_link, final_save_path, reporthook=self.report)
			while os.path.getsize(final_save_path)<500:
				print "\n第二遍写入......."
				urllib.urlretrieve(download_link, final_save_path, reporthook=self.report)
			sys.stdout.flush()

    def mongodb_connection(self):
		client = MongoClient( "localhost", 27017)
		db = client.nasacdf
		fs = gridfs.GridFS(db,  collection='bigfiles')
		return fs

    def put_into_mongodb(self, file_path, file_name, download_link):
		fs = self.mongodb_connection()
		gf = None
		data = None
		try:
			data = open(os.path.join(file_path, file_name),'rb').read()
			gf = fs.put(data, filename=file_name, download_link = download_link)
			#print fs.list()
			print 'file %s has been saved into mongoDB' %file_name
		except Exception,e:
			print "Exception : %s " % e
		return gf

    def get_saved_info(self, file_id):
		fs = self.mongodb_connection()
		gf = None
		try:
			gf  = fs.get(ObjectId(file_id))
			im = gf.read()     
			dic = {}
			dic["length"] = gf.length
			dic["name"] = gf.name
			dic["chunk_size"] =  gf.chunk_size
			dic["content_type"] = gf.content_type
			dic["upload_date"] = gf.upload_date.strftime('%Y-%m-%d')
			dic["md5"] = gf.md5
			dic["download_link"] = gf.download_link
			return (im , dic)
		except Exception,e:
			print "Exception : %s " % e
			return (None,None)

    def parse(self, response):
		#self.log('Hi, this is an item page! %s' % response.url)
		url_list = response.xpath('//td[a]//@href').extract()
		del url_list[0]
		for each_href in url_list:
			#print each_href
			if each_href[-4:]=='.cdf':
				file_name = each_href
				download_link = response.url + file_name
				save_path = response.url[31:]
				final_save_path = self.file_path + save_path
				saved_as = final_save_path + file_name
				print "the download link is: " + download_link
				print "the relative path is: " + save_path
				print "file will be saved as\n" + saved_as
				if not os.path.isfile(saved_as):
					print "=====================download begin========================="
					sys.stdout.write('\rFetching ' + file_name + '...\n')
					self.download_file(download_link, saved_as, file_name)
					file_id = self.put_into_mongodb(final_save_path, file_name, download_link)
					data,dic = self.get_saved_info(file_id)
					print dic
					self.producer(dic)
				else:
					file_id = self.put_into_mongodb(final_save_path, file_name, download_link)
					data,dic = self.get_saved_info(file_id)
					print dic
					self.producer(dic)
			elif each_href[-1:]=='/':
				download_link = response.url + each_href
				#print "the download link is: " + download_link
				save_path = download_link[31:]
				#print "the relative path is: " + save_path
				save_path = self.file_path + save_path
				#print "the following file path will be created:\n " + save_path
				if not os.path.exists(save_path):
					os.makedirs(save_path)
				yield Request(download_link, callback = self.parse)

