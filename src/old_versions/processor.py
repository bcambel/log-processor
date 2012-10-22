#!/usr/bin/env python
import os
import hashlib
import signal
from uasparser import UASparser
import httpagentparser
import threading
from multiprocessing import Pool
import time
from Queue import Queue
print "Welcome"

file_name = "/Users/bcambel/Downloads/logs/wac_0394_20121015_0042.log"

output = open("/Users/bcambel/Downloads/logs/wac_0394_20121015_0042.output2.log","w+")
agents_file = open("/Users/bcambel/Downloads/logs/agents.log","w+")
uas_parser = UASparser('/Users/bcambel/Downloads/logs/cache')
# agents = {}
line_count = 0
count = 0 

reader_queue = Queue()
# writer_queue = Queue()

def worker(line):
	global count,line_count,reader_queue,writer_queue
	
	# line = reader_queue.get()
	# if not line: 
		# time.sleep(0.1)
		# continue
	line_count = line_count + 1
# > 	line = myfile.readline()
	parts = line.split()
	if not (parts[3] == "4069"):
		return
		# output.write(os.linesep)

	count += 1
	agent = " ".join(parts[14:-1])
	# parsed_ua = uas_parser.parse(agent,entire_url='ua_icon,os_icon')
	# parsed_ua = parsed_ua.get('os_family',"")
	parsed_ua = "-".join(httpagentparser.simple_detect(agent))
	# print parsed_ua
	# exit()
	hashed_agent = hashlib.md5(agent).hexdigest()
	# agents[hashed_agent] = agent
	other = parts[:14]
	# writer_queue.put(" ".join( [parts[0], parsed_ua ,parts[2], hashed_agent,os.linesep] ))
	output.write(" ".join( [parts[0], parsed_ua ,parts[2], hashed_agent,os.linesep] ))
	return

def file_reader():
	global reader_queue
	with open(file_name) as myfile:
		myfile.next()
		for line in myfile:
			# reader_queue.put(line)
			pool.apply_async(worker,args=(line,),callback=None)
		
	pool.close()
	pool.join()
	print "File reading is done!"
	
	print "Line count is %d vs %d" % (line_count,count)
		# output.flush()
		# os.fsync(output)
# def file_writer():
# 	global writer_queue,output
# 	while True:
# 		
# 		to_write = writer_queue.get()
# 		if not to_write:
# 			time.sleep(0.1)
# 		else:
# 			output.write(to_write)




reader_thread = threading.Thread(target=file_reader)
pool = Pool(processes=4) 

# worker_thread = threading.Thread(target=worker)
# writer_thread = threading.Thread(target=file_writer)	

def signal_handler(signal, frame):
	print "Handling signal"
	print signal,frame

	if reader_thread is not None:
		reader_thread.join(3)
	# if pool is not None:
		# reader_thread.join(3)	
	# if writer_thread is not None:
	# 	writer_thread.join(3)
	sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

print "Starting Reading Thread"
# reader_thread.start()
file_reader()
print "Starting Worker Thread"
# pool.apply_async(worker) 
# worker_thread.start()
# print "Starting Writer Thread"
# writer_thread.start()

# agent_count = 0 
# for k,v in agents.items():
# 	agent_count += 1
# 	agents_file.write(" ".join([k,v,os.linesep]))
	
# print "Agent count is %d" % agent_count

# for line in myfile.readline():
# 	print line