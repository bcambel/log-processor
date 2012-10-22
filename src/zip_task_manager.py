import subprocess
import time
import sys
import signal
from informant import Informant
import os
from model import Model
from subprocess import Popen
from multiprocessing import Pool
from datetime import datetime as dt
import logging
import re
from args import parse_arguments
from utils import process_running

logging.basicConfig(format='%(asctime)s|%(levelname)s%(message)s',level=logging.NOTSET)

def unzip_file(dir_zip_tuple):

	directory,zip_file,existing_id = dir_zip_tuple
	logging.info( "{0}Welcome to Zipping of {1}{0}".format(10 * "=",zip_file))
	log_collection = Model.connection().logs
	log_file = zip_file[:-3]
	logging.info("Opening process of %s" % zip_file)
#	print existing_id


	process_id = -1
	zip_proc = Popen(["gzip","-d",directory+zip_file],shell=False,stderr=subprocess.STDOUT)
	process_id = zip_proc.pid

	if existing_id is None:
		logging.info( "Our file %s doesn't exist before" % zip_file)
		zip_file_id = log_collection.insert(dict(zip_file=zip_file,status="init",zip_proc=process_id,zip_start=dt.utcnow()))
	else:
		zip_file_id = Model.mongo_id(existing_id)
		logging.info("Our file %s EXISTED before %s" % (zip_file,existing_id))
		log_collection.update({"_id":zip_file_id},{"$set":dict(status="init",zip_proc=process_id,zip_start=dt.utcnow())})


	logging.info( " WAIT BEGINS UnZip processing (%s) started on %s" % (zip_file,process_id))
	zip_proc.wait()

	logging.info("UnZip processing (%s) finished with %s" % (zip_proc.pid,zip_proc.returncode))
	return_code = zip_proc.returncode

	if return_code == 0:
		logging.info( "UnZip(%s) for (%s) completed successfully" % (zip_file,existing_id))
		zip_status = "ready"
	elif return_code > 0:
		logging.info("UnZip(%s) return code is %d" % (zip_file,return_code))

		zip_status = "zip-failed"
	else:
		logging.info( "UnZip(%s) FAILED! Return code is %d" % (zip_file,return_code))
		zip_status = "zip-failed"

	log_collection.update({"_id":zip_file_id},{"$set": dict(file=log_file,status=zip_status,zip_code=return_code,zip_finish=dt.utcnow())})

	return zip_file

#def file_worker(directory,file):
#	ftw = FileTaskWorker()
#	ftw.doJob(directory,file)



class ZipTaskManager(Model,Informant):
	"""
	- once initialized check the log folders for zip files.
	- control at some frequent the LOG folder
	- control the running tasks statuses
	- connect to database to see the uncompleted jobs.
	- connect to database to existing jobs
	"""
	def __init__(self,target_folder,cpu):
		Informant.__init__(self,"ZTM")

		self.__folder_to_watch__ = target_folder
		self.__zipper_pool__ = Pool(processes=1)
		self.logs_collection =  self.__class__.connection().logs
		self.working = True
		
	def loop(self):
		while self.working:
			self.inform( "Refreshing zip file statuses" )
			self.navigate_files()
			time.sleep(10)
			
	def stop(self):
		self.working = False
		self.__zipper_pool__.close()
		self.__zipper_pool__.join()


	def delete_old_file(self,zip_file):
		self.inform( "Deleting old file for %s" % zip_file )
		log_file = zip_file[:-3]
		os.remove(self.__folder_to_watch__+log_file)

	def navigate_files(self):
		zip_files=[]
		try:
			zip_files = [f for f in os.listdir(self.__folder_to_watch__) if re.match(r'.+log.gz',f)]
		except OSError,e:
			logging.exception(e)
			self.inform("%s seems to have a problem. Does it exist?" % self.__folder_to_watch__)
			return

		if len(zip_files) <= 0 :
			self.inform("Boy, we don't have any zip files in the folder." )
			return
		else:
			self.inform( "Found following files %r" % zip_files )
		
		zip_files_cursor = self.logs_collection.find({ "zip_file": { "$in" : zip_files } })
		
		not_processed_zip_files = []
		existing_zip_files = [ ]
		
		for zip_file in zip_files_cursor:
			existing_zip_files.append( (zip_file.get("status"),zip_file.get("zip_file"), str(zip_file.get("_id")),zip_file.get("zip_proc") ))
		
		# check the status of the existing file to not to unzip again if it's processing!
		for zipped in zip_files:
			shouldProcess = True
			existing_id = None
			for s,zip,id,proc_id in existing_zip_files:
				if zip == zipped:
					existing_id = id
					if s in ["done","ready"]:
						self.inform( "FAIL! Zip should be removed already. Skip %s Status => %s" % (zip,s))
						shouldProcess = False
						break
					if s in ["scheduled"]:
						self.inform( "Unzip is scheduled. Be patient :) %s" % zip)
						shouldProcess = False
						break
					if s in ["init"]:
						self.inform("(%s) - Is initialized checking run state" % zip)
						shouldProcess = not process_running(proc_id)
						break
					if s in ["zip-failed"]:
						self.inform("UnZip(%s) failed before! %s" % (zip,s))
						self.delete_old_file(zip)
						shouldProcess = True
						break

			if shouldProcess:
				not_processed_zip_files.append((zipped,existing_id))

		if len(not_processed_zip_files) > 0 :
			self.inform("Will be processing \n%r" % not_processed_zip_files)

			for z,existing_id in not_processed_zip_files:
				if existing_id is None:
					existing_id = self.logs_collection.insert(dict(zip_file=z,status="scheduled",scheduled_at=dt.utcnow()))
					self.place_job_to_queue(z,existing_id)
#				unzip_file((self.__folder_to_watch__,z,existing_id))


#			pool_handle.wait()

		self.inform( "Job has been completed..." )

	def place_job_to_queue(self,zip,existing_id):
		self.inform(u"PLacing %s-(%s) to job schedule" % (zip,existing_id))
		self.__zipper_pool__.apply_async(unzip_file,args=((self.__folder_to_watch__,zip,existing_id),))

if __name__ == "__main__":

	args = parse_arguments()

	DIR = args.input_dir
	max_worker = args.max_proc

	ztm = ZipTaskManager(DIR,4)

	def signal_handler(signal, frame):
		print "Handling signal"
		print signal,frame
		ztm.stop()
		sys.exit(0)

	signal.signal(signal.SIGINT, signal_handler)



	ztm.loop()