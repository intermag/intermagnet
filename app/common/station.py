#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-04-03 17:20:55
# @Author  : Your Name (you@example.org)
# @Link    : http://example.org
# @Version : $Id$

import os
import base_conn
from flask import Flask,jsonify
#app = Flask(__name__)

#@app.route('/')
def get_list_by_cursor(cursor):
	list = []
	for x in cursor:
		list.append(x)
	return list
def get_list_by_cursor_values(cursor):
	list = []
	for x in cursor:
		list += (x.values())
	return list
def get_all_stations():
	conn = base_conn.get_conn('station')
	cursor = conn.find({},{'_id':0,'name':1})
	stations = get_list_by_cursor_values(cursor)
	count = len(stations)
	return {'stations':stations,'count':count}
	#return jsonify({'stations':stations})
	#return jsonify({'a':'dsaf'})
#print get_all_stations()

# def get_stations_count():
# 	stations_count = len(base_conn.get_collection_names())
# 	return {'stations_count':stations_count}

def get_station_count(station_name):
	station_count = base_conn.get_conn(collection_name=station_name).find().count()
	return {'count':station_count}

def get_list_by_station_time(station_name,starttime,endtime):
	conn = base_conn.get_conn(station_name)
	cursor = conn.find({
		'$and':[
			{'STATION':station_name},
			{'DATE':{'$gte':starttime,'$lte':endtime}}
		]
	},{'_id':0,'TIME':1,'X':1,'Y':1,'Z':1})
	#cursor = conn.find({'STATION':station_name,'DATE':{'$gte':starttime,'$lte':endtime}},{'_id':0})
	list = get_list_by_cursor(cursor)
	count = len(list)
	return {'list':list,'count':count}

def get_stations_by_location(x_latitude,y_latitude,x_longitude,y_longitude):
	conn = base_conn.get_conn('station')
	cursor = conn.find({
		'$and':[
			{'latitude':{'$gte':x_latitude,'$lte':y_latitude}},
			{'longitude':{'$gte':x_longitude,'$lte':y_longitude}}
		]
		},{'_id':0,'name':1})
	stations = get_list_by_cursor_values(cursor)
	count = len(stations)
	return {'stations':stations,'count':count}

