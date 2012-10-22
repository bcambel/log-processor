
"""
FILE TASK WORKER

"""
from Queue import Queue
import logging
import time
from informant import Informant
from model import Model
import os
import httpagentparser
import hashlib
from datetime import datetime as dt
from stoppable_thread import StoppableThread

logging.basicConfig(format='%(asctime)s|%(levelname)s%(message)s',level=logging.NOTSET)

class FileTaskWorkerHBeat(StoppableThread,Informant):
	def __init__(self,file_worker,sec=10):

		StoppableThread.__init__(self)
		Informant.__init__(self,"BEAT")

		if file_worker is not None:
			self.file_worker = file_worker
		else:
			raise Exception("File Worker is None")

		self.__sleep_seconds = sec

	def run(self):
		self.name = "BEAT-%s" % self.file_worker.file

		self.inform("Heart Beat Started %s. Sleep 3 seconds " )

		while self.stop_event.is_set() == False:
			time.sleep(self.__sleep_seconds)
			self.file_worker.heart_beat()

		if hasattr(self,"__process_id"):
			self.inform( "Exiting Heart Beat thread for %s" % self.__process_id )


class FileTaskWorker(Model,Informant):
	"""
	FileTaskWorker handles the map-reduce operation of the LOG files.

	Each multiprocessing.Pool process creates a single instance of the FileTaskWorker

	"""
	def __init__(self,DIR,log_file,flush_freq):

#		StoppableThread.__init__(self)
		Informant.__init__(self,"FTW")

		self.line_cursor = 0
		self.__file_id__ = None
		self.logs_collection = self.__class__.connection().logs
		self.__working = False
		self.eligible_count = 0
		self.__log_file = log_file
		self.__log_file_fpath = DIR+log_file

		self.flush_freq = flush_freq
		self.reduced_log_file= self.__log_file_fpath + ".output"
		self.reduced_log_file_info = "%s.info.log" % self.reduced_log_file
		self.reduced_log_stream = None
		self.__agent_cache = {}

		self.__reduce_queue = Queue()
		self.__output_queue = Queue()

	@property
	def file(self):
		return self.__log_file

	def heart_beat(self):
		self.inform( "Updating %s - %s to %d" % ( self.__log_file, self.__file_id__, self.line_cursor ) )
		self.update_log_db_doc({ "loc" : self.line_cursor , "hbeat": dt.utcnow() })

	def update_log_db_doc(self,setOperator):
		self.logs_collection.update({"_id" : self.__file_id__ }, { "$set": setOperator } ,safe=True)

	def run(self):
		self.inform("Running for %s" % self.__log_file)
		self.__working = True

		self.find_file_info_doc()

		if not self.validate_file_against_db_doc():
			self.inform("Cannot validate File(%s) status on db. EXITing processing" % self.__log_file)
			return False

		self.inform( "Extract %s" % self.__log_file_fpath )

		self.__set_file_status_working__()

		self.__time_start = dt.utcnow()
		self.map_reduce_log_lines( )
		self.__time_end = dt.utcnow()

		self.write_results_info()



		self.__working = False
		return True


	def stop(self):
		self.inform("Received stop command for processor: %s" % self.__log_file)
		self.update_log_db_doc(dict(status="stop"))



	def __reduce__(self,parts):
		self.eligible_count += 1
		agent = " ".join(parts[14:-1])
		hashed_agent = hashlib.md5(agent).hexdigest()

		if self.__agent_cache.has_key(hashed_agent):
			parsed_ua = self.__agent_cache.get(hashed_agent,"")
		else:
			parsed_ua = "-".join(httpagentparser.simple_detect(agent))
			self.__agent_cache[hashed_agent] = parsed_ua

		output_line = " ".join([str(self.line_cursor), parts[0], parsed_ua, parts[2], hashed_agent, os.linesep])
		self.__output_queue.put(output_line)
#		self.reduced_log_stream.write()

	def __reduce_queue__(self):
		while self.__reduce_queue._qsize() > 0:
			self.__reduce__(self.__reduce_queue.get())

		while self.__output_queue._qsize() > 0:
			self.reduced_log_stream.write(self.__output_queue.get())

		self.reduced_log_stream.flush()


	def __map__(self,log_stream):
		log_stream.next()

		for line in log_stream:
			self.line_cursor += 1

			parts = line.split()
			if not (parts[3] == "4069"):
				continue

			self.__reduce_queue.put(parts)

			if (self.line_cursor % self.flush_freq) == 0:
				self.__reduce_queue__()

		self.__reduce_queue__()

	def map_reduce_log_lines(self):
		self.line_cursor = 0

		self.reduced_log_stream = file(self.reduced_log_file,"w+")

		with open(self.__log_file_fpath, "rb") as log_stream:
			self.__map__(log_stream)

		self.logs_collection.update({"_id": self.__file_id__},
					{"$set": {"loc": self.line_cursor, "status": "done", "done_at": dt.utcnow()}})

		self.reduced_log_stream.flush()

	def __set_file_status_working__(self):
		self.logs_collection.update({"_id": self.__file_id__}, {"$set": {"status": "working","hbeat":dt.utcnow()}})

	def find_file_info_doc(self):
		self.db_file_info = self.logs_collection.find_one({"file":self.__log_file})

	def validate_file_against_db_doc(self):
		if self.db_file_info is None:
			# we must already have the file already created
			self.__file_id__ = self.logs_collection.insert({ "file": self.__log_file , "loc":0 ,"status":"start" })
			return False
		else:
			self.inform( "Using existing ID for %s " % self.__log_file_fpath )
			self.__file_id__ = self.db_file_info.get("_id")
			loc = self.db_file_info.get("loc",0)
			if loc != 0:
				self.inform("This(%d) was the location of the processing before " % loc )

			status = self.db_file_info.get("status")

			if status == "done":
				self.inform( "File already done %r" % self.db_file_info )
				self.inform( "Will exist JOB" )
				return False
			if status == "stop":
				self.inform("Stop operation command %r" % self.db_file_info )
				return False

			if status == "re-init":
				self.inform( "Reinit Processor %s " % self.__log_file_fpath )
			return True

	def write_results_info(self,):
		try:

			how_long = str((self.__time_end - self.__time_start))
			self.inform(  "###DONE %s completed in %s Line Count %d vs %d" % (self.__log_file,how_long,self.line_cursor,self.eligible_count) )

			with file(self.reduced_log_file_info, "w+") as output_info:
				output_info.write("Started: %s\n" % self.__time_start)
				output_info.write("Ended: %s\n" % self.__time_end)
				output_info.write("Duration: %s\n" % how_long)
				output_info.write("Log file has %d\n" % self.line_cursor)
				output_info.write("Outputted %d\n" % self.eligible_count)

		except IOError as e:
			logging.exception(e)