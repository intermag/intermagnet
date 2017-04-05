#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-04-04 14:55:59
# @Author  : Your Name (you@example.org)
# @Link    : http://example.org
# @Version : $Id$

import os
import sys
sys.path.append('../')
#from common import station
from common.station import get_all_stations

print get_all_stations()
# print get_stations_count()
# print get_station_count('Kourou')
# print get_list_by_station_time(station_name='Kourou', starttime='2017-03-15', endtime='2017-03-20')
# print get_stations_by_location(4,6,307,345)

