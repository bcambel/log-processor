import logging
from bson.errors import InvalidId
from bson.objectid import ObjectId
import pymongo

db_host = "localhost"
connection=None;db = None 

def get_connection():
	if db is None:
		initialize()
	return db
	
def initialize(is_production=None):
	global db_host,connection,db

	connection = pymongo.Connection(db_host,27017,auto_start_request=False)
	db = connection.youngsons

class Model(object):
	
	@classmethod
	def connection(cls):
		db = get_connection()
		return db
	@classmethod
	def mongo_id(cls,id):
		object_id = None

		if id is None:
			return None

		if isinstance(id,ObjectId):
			object_id  = id
		elif isinstance(id,basestring) and id not in ["None",""]:
			try:
				object_id  = ObjectId(id)
			except  InvalidId,inv:
				logging.exception(inv)

		return object_id