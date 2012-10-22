#!bin/python
import logging

from multiprocessing import Pool
from args import parse_arguments
import time
import sys
import signal
from file_task_worker import FileTaskWorker, FileTaskWorkerHBeat
from informant import Informant, inform
from model import Model
from utils import five_mins_later
from datetime import datetime as dt, timedelta
import atexit

#logger = multiprocessing.log_to_stderr(multiprocessing.SUBDEBUG)
#logger.warn("Multiprocessing Logger level to INFO")

logging.basicConfig(format='%(asctime)s|%(levelname)s%(message)s',level=logging.NOTSET)



def start_working(file):
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	inform( "Should be working on %s" % file)
	ftw = FileTaskWorker(DIR,file,flush_frequency)
	hbeat = FileTaskWorkerHBeat(ftw)
	result = False

	try:
		hbeat.start()
		result = ftw.run()

		hbeat.stop()

	except KeyboardInterrupt:
		inform( "Handling keyboard interrupt" )
		ftw.stop() if ftw is not None else None
		result = False
	except Exception,e:
		print e
		result = False
	finally:
		try:
			hbeat.stop()
			hbeat.join(timeout=1)
		finally:
			return result


class FileTaskMonitor(Informant,Model):

	def __init__(self,workers=4):
		Informant.__init__(self,"MONIT")

		self.__logs_collection__ = self.__class__.connection().logs
		self.__max_workers = workers
		self.__worker_pool__ = None
		self.__workers = []
		self.__working = False

	def queue_available(self):
		qsize = self.__worker_pool__._taskqueue.qsize()
		self.inform("Queue Length is %d vs %d" % (qsize,self.__max_workers))
		if qsize < self.__max_workers:
			return True
		else:
			return False

	def visit_workers_status(self):
		self.inform("Checking JOB Statuses %d" % len(self.__workers))
		idx = 0
		unfinished_jobs_count = 0

		for id,file,w in self.__workers:
			ready = w.ready()
			if not w.ready():
				successful = w.successful() if ready else False
				value = w._value if ready and successful and hasattr(w,"_value") else None
#				self.inform("Id:%s,F:(%s),Job:%s,R:%s,S:%s,V:%s" % (str(id),file,w._job, ready, successful, value ))
				unfinished_jobs_count += 1
				idx += 1
			else:
				del self.__workers[idx]

		self.inform("There is %d jobs left" % unfinished_jobs_count)


	def check_new_files_to_be_processed(self):
		self.inform( "{0}Checking New Files to Process{0}".format("="*10))

		logs_cursor = self.__logs_collection__.find({"status":"ready"})
		count = 0

		for file in logs_cursor:
			count += 1
			self.__logs_collection__.update({"_id":file.get("_id")},{"$set":{"status":"proc_schedule","proc_timeout":five_mins_later()}},safe=True)
			self.schedule_job(file.get("_id"),file.get("file"))

		self.inform( "Found %s files to process " % ( str(count) if count > 0 else "NO" ) )

	def schedule_job(self,file_doc_id,file):
		self.inform("Scheduling jobs for %s" % file )
		result = self.__worker_pool__.apply_async(start_working,args=(file,))
		self.__workers.append((file_doc_id,file,result))
#		result.get(999999)

	def check_scheduled_but_left_behind_jobs(self):
		self.inform( "{0}Checking Left Behind Tasks to Process{0}".format("="*10))

#		qsize = self.__worker_pool__._taskqueue.qsize()
#		self.inform("Process Queue Length is %s" % (qsize))

		query = { "status":{"$in": ["proc_schedule","working"] } }

		count = self.__logs_collection__.find(query).count()

		if count <= 0:
			self.inform( "Nothing seems to be active or left behind" )
		else:
			self.inform("Possibly %d working. Let's check their status" % count)

		logs_cursor = self.__logs_collection__.find(query)
		def_timeout = dt.utcnow() - timedelta(hours=1)

		for log in logs_cursor:
			shouldReschedule = False
			file = log.get("file")
			status = log.get("status")
			hbeat = log.get("hbeat")
			timeout = log.get("proc_timeout",def_timeout)

			if status in ["working"] :
				if hbeat is None:
					shouldReschedule = True
				elif hbeat < dt.utcnow() - timedelta(minutes=1):
					shouldReschedule = True

			elif status in ["proc_schedule"] and timeout <= dt.utcnow():
				shouldReschedule = True

			if shouldReschedule:
				self.inform("Rescheduling %s - was %s" % (file,status))
				self.__logs_collection__.update({"_id":log.get("_id")},{"$set":{"status":"proc_schedule","proc_timeout":five_mins_later()}},safe=True)
				self.schedule_job(log.get("_id"),file)

	def check_scheduled_but_unprocessed_items(self):
		query = {"status":{"$in":["proc_schedule","quit","stop"] }}
		count = self.__logs_collection__.find(query).count()
		self.inform("Found %d scheduled but Unprocessed items" % count)
		if count <= 0 :
			return

		logs_cursor = self.__logs_collection__.find(query)

		for log in logs_cursor:
			file = log.get("file")
			self.__logs_collection__.update({"_id":log.get("_id")},{"$set":{"status":"proc_schedule","proc_timeout":five_mins_later()}},safe=True)
			self.schedule_job(log.get("_id"),file)

	def begin(self):
		self.__working = True
		self.__worker_pool__ = Pool(processes=self.__max_workers)
		self.check_scheduled_but_unprocessed_items()

		while True:
			self.check_scheduled_but_left_behind_jobs()

			self.check_new_files_to_be_processed()

			self.visit_workers_status()

			time.sleep(10)

	def stop(self):
		if not self.__working:
			return

		self.inform("Stopping Monitor")
		self.__worker_pool__.close()
		self.__worker_pool__.terminate()

		self.update_jobs_status()

		self.inform("Processing Pool Stopped")

	def update_jobs_status(self):
		files_to_failed = []
		log_ids = []

		for id,file,result in self.__workers:
			if not result.ready() :
				files_to_failed.append((id,file))
				log_ids.append(id)

		self.inform("update status of %r" % files_to_failed)

		self.__logs_collection__.update(
				{
					"_id": {"$in" : log_ids } , "status" : {"$nin" : ["done"] }
				},
				{
					"$set" : { "status" : "quit" }
				}, multi=True,upsert=False,safe=True

		)



def cleanup():
	inform("~~~~~~~~EXITING - Cleaning up~~~~~~~~~~~")
	try:
		if monitor is not None:
			monitor.stop()
	except Exception,e:
		inform("Exception during stopping monitor %s " % e)
	finally:
		inform("BYEEE!")

atexit.register(cleanup)



if __name__ == "__main__":
	global monitor
	global DIR
	global flush_frequency

	monitor = None
	signals_to_handle = [signal.SIGINT,signal.SIGHUP,signal.SIGQUIT,signal.SIGABRT,signal.SIGALRM]
	args = parse_arguments()
	DIR = ""
	flush_frequency = args.flush
	monitor = FileTaskMonitor(workers=args.max_proc)

	def signal_handler(signal, frame):
		inform( "Handling signal %s %s"%(signal,frame ))
#		monitor.stop()
		sys.exit(0)


	DIR = args.input_dir


	for os_signal in signals_to_handle:
		signal.signal(os_signal, signal_handler)

	inform( "OS_SIGNALS Registered" )

	inform( "Listening and Writing Directory set to %s" % DIR )

	try:
		monitor.begin()
	except KeyboardInterrupt:
		inform( "[MONIT] Monitor Handled KeyboardInterrupt")
	except Exception, e:
		inform('[MONIT] got exception: %r, terminating the pool' % (e,))
		try:
			monitor.stop()
		except Exception,e :
			print e
		finally: pass
	inform('Pool is terminated')
