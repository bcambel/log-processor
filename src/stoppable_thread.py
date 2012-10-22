import logging
import os
import threading

class StoppableThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.stop_event = threading.Event()

	def stop(self):
		if self.isAlive() == True:
			logging.info( u"~~~##[THR:%s] Stopping thread %s~~~" % ( os.getpid(),self.name ))
			# set event to signal thread to terminate
			self.stop_event.set()
			# block calling thread until thread really has terminated
			self.join()
