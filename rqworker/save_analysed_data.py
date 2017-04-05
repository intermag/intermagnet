import pymongo

def save_data(data):
	try:
		client = pymongo.MongoClient()
		db = client.intermagnet
		coll = db.intermag_data
	except Exception, e:
		print 'connect to intermag_data failed......'
	try:
		coll.save(dict(
					unit_col_z = data['unit-col-z'], 
					unit_col_x = data['unit-col-x'],
					unit_col_y = data['unit-col-y'], 
					col_z = data['col-z'],
					col_f = data['col-f'], 
					DataSamplingFilter = data['DataSamplingFilter'],
					StationInstitution = data['StationInstitution'],
					StationIAGAcode = data['StationIAGAcode'],
					col_x = data['col-x'], 
					StationID = data['StationID'],
					DataFormat = data['DataFormat'],
					DataPublicationLevel = data['DataPublicationLevel'], 
					StationName = data['StationName'], 
					DataAcquisitionLongitude = data['DataAcquisitionLongitude'], 
					col_y = data['col-y'], 
					DataAcquisitionLatitude = data['DataAcquisitionLatitude'], 
					unit_col_f = data['unit-col-f'], 
					DataComponents = data['DataComponents'],
					Datas = data['Datas'],
					))
		print 'data saved successfully......'
	except Exception, e:
		print 'data saving failed......'
	client.close()
	return
