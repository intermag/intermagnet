#-*-coding:utf8-*-
"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""

from io import open

from datetime import datetime

def readIAGA(filename):

    fh = open(filename, 'rt')
    #diction 存放解析文件信息
    diction={}

    for line in fh:
        if line.startswith(' '):
            infoline = line[:-4]
            key = infoline[:23].strip()
            val = infoline[23:].strip()
            # print key+val
            if key.find('Source') > -1:
                if not val == '':
                    diction['StationInstitution'] = val
            if key.find('Station') > -1:
                if not val == '':
                    diction['StationName'] = val
            if key.find('IAGA') > -1:
                if not val == '':
                    diction['StationIAGAcode'] = val
                    diction['StationID'] = val
            if key.find('Latitude') > -1:
                if not val == '':
                    diction['DataAcquisitionLatitude'] = val        
            if key.find('Longitude') > -1:
                if not val == '':
                    diction['DataAcquisitionLongitude'] = val
            if key.find('Elevation') > -1:
                if not val == '':
                    diction['DataElevation'] = val
            if key.find('Format') > -1:
                if not val == '':
                    diction['DataFormat'] = val
            if key.find('Reported') > -1:
                if not val == '':
                    diction['DataComponents'] = val
            if key.find('Orientation') > -1:
                if not val == '':
                    diction['DataSensorOrientation'] = val
            if key.find('Digital') > -1:
                if not val == '':
                    diction['DataDigitalSampling'] = val
            if key.find('Interval') > -1:
                if not val == '':
                    diction['DataSamplingFilter'] = val
            if key.startswith(' #'):
                if key.find('# V-Instrument') > -1:
                    if not val == '':
                        diction['SensorID'] = val
                elif key.find('# PublicationDate') > -1:
                    if not val == '':
                        diction['DataPublicationDate'] = val
                else:
                    print ("formatIAGA: did not import optional header info {a}".format(a=key))
            if key.find('Data Type') > -1:
                if not val == '':
                    if val[0] in ['d','D']:
                        diction['DataPublicationLevel'] = '4'
                    elif val[0] in ['q','Q']:
                        diction['DataPublicationLevel'] = '3'
                    elif val[0] in ['p','P']:
                        diction['DataPublicationLevel'] = '2'
                    else:
                        diction['DataPublicationLevel'] = '1'
            if key.find('Publication Date') > -1:
                if not val == '':
                    diction['DataPublicationDate'] = val
        elif line.startswith('DATE'):
            colsstr = line.lower().split()
            # print colsstr
            diction['Datas'] = {colsstr[0]:"",colsstr[1]:"",colsstr[2]:"",colsstr[3]:"",colsstr[4]:"",colsstr[5]:"",colsstr[6]:""}
            varstr = ''
            for it,elem in enumerate(colsstr):
                if it > 2:
                    varstr += elem[-1]
            varstr = varstr[:4]
            # print varstr
            diction["col-x"] = varstr[0].upper()
            diction["col-y"] = varstr[1].upper()
            diction["col-z"] = varstr[2].upper()
            diction["unit-col-x"] = 'nT'
            diction["unit-col-y"] = 'nT'
            diction["unit-col-z"] = 'nT'
            diction["unit-col-f"] = 'nT'
            if varstr.endswith('g'):
                diction["unit-col-df"] = 'nT'
                diction["col-df"] = 'G'
                diction["col-f"] = 'F'
            else:
                diction["col-f"] = 'F'
            if varstr in ['dhzf','dhzg']:
                #diction["col-x"] = 'H'
                #diction["col-y"] = 'D'
                #diction["col-z"] = 'Z'
                diction["unit-col-y"] = 'deg'
                diction['DataComponents'] = 'HDZF'
            elif varstr in ['ehzf','ehzg']:
                #diction["col-x"] = 'H'
                #diction["col-y"] = 'E'
                #diction["col-z"] = 'Z'
                diction['DataComponents'] = 'HEZF'
            elif varstr in ['dhif','dhig']:
                diction["col-x"] = 'I'
                diction["col-y"] = 'D'
                diction["col-z"] = 'F'
                diction["unit-col-x"] = 'deg'
                diction["unit-col-y"] = 'deg'
                diction['DataComponents'] = 'IDFF'
            elif varstr in ['hdzf','hdzg']:
                #diction["col-x"] = 'H'
                #diction["col-y"] = 'D'
                diction["unit-col-y"] = 'deg'
                #diction["col-z"] = 'Z'
                diction['DataComponents'] = 'HDZF'
            else:
                #diction["col-x"] = 'X'
                #diction["col-y"] = 'Y'
                #diction["col-z"] = 'Z'
                diction['DataComponents'] = 'XYZF'
        else:
            # data entry - may be written in multiple columns
            # row beinhaltet die Werte eine Zeile
            row=[]
            # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
            # da darin der Zeilenumbruch "\n" steht
            for val in line[:-1].split():
                # nur nicht-leere Spalten hinzufuegen
                if val.strip()!="":
                    row.append(val.strip())
            diction['Datas'] = str({colsstr[0]:row[0],colsstr[1]:row[1],colsstr[2]:row[2],colsstr[3]:row[3],colsstr[4]:row[4],colsstr[5]:row[5],colsstr[6]:row[6]})+","+str(diction['Datas'])

    return diction