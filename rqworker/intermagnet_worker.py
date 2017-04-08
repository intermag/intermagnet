#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pika
import time
import json
import zipfile
import gzip
import os
import connect_mongoDB
import pymongo
import hashlib
import format_iaga02
import save_analysed_data
from pymongo import MongoClient

class IntermagWorker():

    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        channel = connection.channel()
        result = channel.queue_declare(
            queue='intermagnet_task_queue', durable=True)
        self.callback_queue = result.method.queue
        print ' [*] Waiting for messages. To exit press CTRL+C'
        channel.basic_consume(self.on_response,queue=self.callback_queue)
        channel.start_consuming()

    def read_context(self, file_path):
        file = open(file_path)
        context = file.read()
        return context

    def store_file_into_mongoDB(self, file_infos):
        try:
            client = MongoClient('localhost', 27017)
            db = client.intermagnet
            coll = db.intermag_files
            print 'Connect to intermag_files in mongoDB...'

            coll.save(dict(
                file_name = file_infos['file_name'], 
                source_date = file_infos['source_date'], 
                file_content = file_infos['file_content'],
                SHA1 = file_infos['SHA1'],
                available = file_infos['available'], 
                format = file_infos['format'],
                region = file_infos['region'], 
                latitude = file_infos['latitude'],
                rate = file_infos['rate'], 
                location = file_infos['location'],
                type = file_infos['type'], 
                obs = file_infos['obs'],
                save_date = file_infos['save_date'],
                ))
            print 'File saved successfully......'
        except Exceptions, e:
            print 'File saved failed......'
        client.close()
        return

    def compute_SHA1(self, filename):
        file=filename
        fd = open(file)
        fd.seek(0)
        line = fd.readline()

        sha1 = hashlib.sha1()
        sha1.update(line)

        while line:
            line = fd.readline()
            sha1.update(line)

        fsha1 = sha1.hexdigest()
        #print fsha1
        return fsha1

    def un_zip(self, file_path, file_name):
        #eg: 
        #file_path='/home/undoingfish/Documents/zip_temp/data20170404052351.zip'
        #file_name='data20170404052351.zip'
        file_read_path = ''
        zip_file = zipfile.ZipFile(file_path)
        if os.path.isdir(file_path[:-4]+'/'): #判断文件夹是否已经存在
            pass
        else:
            save_name = file_name[:-4]
            #eg: save_name=
            #'/home/undoingfish/Documents/zip_temp/data20170404052351.zip'
            print save_name
            os.mkdir(file_path[:-4]+'/')#创建文件夹
        for names in zip_file.namelist():
            zip_file.extract(names,file_path[:-4])
            if names[-3:]=='.gz':
                file_read_path = self.second_decompress(file_path, names)
                #eg: file_path=
                #'/home/undoingfish/Documents/zip_temp/data20170404052351.zip'
                #names='cbb20160101vmin.min.gz'
            if names[-4:]=='.sec':
                file_read_path = file_path[:-4]+'/'+names
            if names[-4:]=='.min':
                file_read_path = file_path[:-4]+'/'+names
        zip_file.close()
        os.remove(file_path)
        print 'file_read_path in un_zip' + file_read_path
        return file_read_path

    def un_gz(self, file_path, file_name):
        #eg: 
        #file_path='/home/undoingfish/Documents/zip_temp/data20170404052351.zip'
        #file_name='cbb20160101vmin.min.gz'
        save_path = file_path[:-4]+'/'+file_name
        #eg: save_path=
        #'/home/undoingfish/Documents/zip_temp/data20170404052351/cbb20160101vmin.min.gz'
        f_name = save_path.replace(".gz", "")
        #eg: save_path=
        #'/home/undoingfish/Documents/zip_temp/data20170404052351/cbb20160101vmin.min'
        g_file = gzip.GzipFile(save_path)
        open(f_name, "wb").write(g_file.read())
        print file_name
        g_file.close()
        os.remove(save_path)
        return f_name

    def first_decompress(self, path, filename):
        file_read_path = self.un_zip(path, filename) 
        #eg: path='/home/undoingfish/Documents/zip_temp/data20170404052351.zip'
        #file_name='data20170404052351.zip'
        print path
        print 'file_read_path in first_decompress' + file_read_path
        return file_read_path

    def second_decompress(self, file_path, file_name):
        if not os.path.isdir(file_path):
            file_read_path = self.un_gz(file_path, file_name)
        print 'file_read_path in second_decompress' + file_read_path
        return file_read_path

    def search_in_mongo(self, file_name):
        try:
            client = MongoClient('localhost', 27017)
            db = client.intermagnet
            coll = db.intermagnet
            print 'Connect to intermagnet in mongoDB...'
        except Exception, e:
            print 'Can not connect to intermagnet in mongoDB...'
        res = coll.find_one({'filename':file_name})
        #print res['download_name']
        client.close()
        return res

    def fetch_out(self, res):
        temp_path = '/home/undoingfish/Documents/zip_temp/'

        save_path = temp_path + res['download_name'] 
        #eg: save_path=
        #'/home/undoingfish/Documents/zip_temp/data20170404052351.zip'
        with open(save_path, 'wb') as f:
            f.write(res['file_content'])
        print 'fetch succes.....'
        print 'saved as %s' %save_path
        file_read_path = self.first_decompress(save_path, res['download_name'])
        print 'file_read_path in fetch_out' +file_read_path
        #os.remove(save_path)
        return file_read_path

    def on_response(self, ch, method, properties, body):
        content = json.loads(body)
        file_infos={
                    'file_name': content['file_name'], 
                    'source_date': content['date'], 
                    'file_content':'',
                    'SHA1':'',
                    'available': content['available'], 
                    'format': content['format'],
                    'region': content['region'], 
                    'latitude': content['latitude'],
                    'rate': content['rate'], 
                    'location': content['location'],
                    'type': content['type'], 
                    'obs': content['obs'],
                    'save_date': '',
                    }
        print(" [x] Received %r" % json.loads(body))
        res = self.search_in_mongo(content['file_name'])
        file_read_path = self.fetch_out(res)
        print 'file decompressed in ......'+file_read_path
        context = self.read_context(file_read_path)
        statinfo = os.stat(file_read_path)
        create_time = statinfo.st_mtime
        timeArray = time.localtime(create_time)
        create_date = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        print create_date
        sha1 = self.compute_SHA1(file_read_path)
        file_infos['file_content'] = context
        file_infos['save_date'] = create_date
        file_infos['sha1'] = sha1
        self.store_file_into_mongoDB(file_infos)
        analysed_data = format_iaga02.readIAGA(file_read_path)
        #print 'analysed_data:   '
        #print analysed_data.keys()
        #print type(analysed_data)
        save_analysed_data.save_data(analysed_data)
        ch.basic_ack(delivery_tag = method.delivery_tag)

intermag = IntermagWorker()

