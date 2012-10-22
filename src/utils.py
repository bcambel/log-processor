from datetime import timedelta,datetime as dt
import logging,os,errno
import multiprocessing

cpu_count= multiprocessing.cpu_count()

def process_running(proc_id):
	try:
		os.kill(proc_id, 0)
		logging.warn("Process is living? %d" % proc_id)
		return False
	except OSError, e:
		logging.warn(e)
		logging.warn("Process isn't living. Will unzip file again %s" % zip)
		if e.errno == errno.EPERM:
			print "There might be a permission error" % e
		return True


def five_mins_later():
	return dt.utcnow()+timedelta(minutes=5)