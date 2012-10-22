import logging
import os

current_process_id = os.getpid()

def inform(msg):
	logging.info( "[MONIT:%d] %s" % (current_process_id,msg) )

class Informant():
	def __init__(self,who):
		self.__process_id = current_process_id
		self.who_am_i = who

	def inform(self,str):
		logging.info( u"[%s:p%d] %s" %(self.who_am_i, self.__process_id,str ))
