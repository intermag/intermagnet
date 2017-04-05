#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-04-02 15:46:02
# @Author  : Your Name (you@example.org)
# @Link    : http://example.org
# @Version : $Id$

import os
from pymongo import MongoClient

mongo_uri_auth = "mongodb://localhost:27017/"
database_name = "app"
collection_name = "Kourou"

def get_db(mongo_uri_auth=mongo_uri_auth,database_name=database_name):
	client = MongoClient(mongo_uri_auth)
	db = client[database_name]
	return db

def get_collection_names(mongo_uri_auth=mongo_uri_auth,database_name=database_name):
	db = get_db()
	return db.collection_names(include_system_collections=False)

def get_conn(collection_name=collection_name):
	dbs = get_db()
	conn = dbs[collection_name]
	return conn
#print get_conn().find().count()
#print get_conn()
#print get_collection_names()
