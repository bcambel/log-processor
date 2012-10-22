#!/usr/bin/env python
import os
import re
import httpagentparser
import hashlib
from subprocess import call
from multiprocessing import Pool
import pymongo

DIR = "/Users/bcambel/Downloads/logs/"
db_host = "localhost"

"""
	At the first moment, look at the database tasks.
	
	Look at the zip files found in the folder.
	Extract zip file
	
	If zip file is not found as a task in the database
		- create a new file task.
	
	IF we have an existing task
		- Get the file info. Last location, status, file, output file
		- Check the output file exist
		- Open the output file and look at the last line to see the last processed line
		- Go to the LOG file's last processed line
		- Keep on processing
"""

def zip_extractor(zip_file):
	log_file = zip_file[:-3]
	call(["gzip","-d",DIR+zip_file])
	
	return log_file
	
def file_processor(log_file):
	"""
	Receive a log file, check if exists in the database
	Find the output file, see the last written line from the log file
	Move to the location. 
	"""
	connection = pymongo.Connection(db_host,27017,auto_start_request=False)
	db = connection.log_processor
	
	file_name = DIR+log_file
	db_file_info = db.logs.find_one({"file":file_name})
	file_id = None
	last_location = 0 
	
	if db_file_info is None:
		file_id = db.logs.insert({ "file": file_name, "loc":0 ,"status":"start" })	
	else:
		file_id = db_file_info.get("_id")
		loc = db_file_info.get("loc",0)
		status = db_file_info.get("status")
		
	print "Extract %s" % file_name
	# return
	if status == "done":
		return

	line_count = 0
	count = 0
	

	
 	output = file(file_name+".output","w+")
	with open(file_name,"rb") as myfile:
		myfile.next()
		for line in myfile:
			
			line_count = line_count + 1
			parts = line.split()
			if not (parts[3] == "4069"):
				continue
			count += 1
			agent = " ".join(parts[14:-1])
			parsed_ua = ""#"-".join(httpagentparser.simple_detect(agent))
			hashed_agent = hashlib.md5(agent).hexdigest()
			other = parts[:14]

			output.write(" ".join( [str(line_count), parts[0], parsed_ua ,parts[2], hashed_agent,os.linesep] ))
			
			if line_count != 0 and line_count % 100 == 0 :
				db.logs.update({"_id" : file_id }, { "$set": { "loc" : line_count }})
				
		db.logs.update({"_id" : file_id }, { "$set": { "loc" : line_count, "status":"done" }})
	
	output.flush()	
	print "Line Count %d vs %d" % (line_count,count)

zip_files = [f for f in os.listdir(DIR) if re.match(r'.+log.gz',f)]

pool = Pool(processes=6)

pool.map(zip_extractor,[zipfile for zipfile in zip_files])

pool.map(file_processor,[zipfile[:-3] for zipfile in zip_files])
	

# for zipfile in zip_files:
	# pool.apply_async(zip_extractor,args=(DIR,zipfile),callback=file_processor).get()

# pool.map(file_processor,["%s%s"% (DIR,myfile) for myfile in zip_files])

# for myfile in zip_files:
# 	current_file =  "%s%s"% (DIR,myfile)
# 	print current_file
# 	pool.apply_async(file_processor,args=(current_file,)).get()

	