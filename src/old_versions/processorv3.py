#!/usr/bin/env python
import os
import time
import httpagentparser
import hashlib
import re
FILE = "/Users/bcambel/Downloads/logs/wac_0394_20121015_0042.log"
output = open("/Users/bcambel/Downloads/logs/wac_0394_20121015_0042.output3.log","w+")

def getchunks(file, size=1024*1024*2):
    # scan a file, and yield sequence of (start, size) chunk descriptors
    # where all chunks contain full lines
    f = open(file, "rb")
    while 1:
        start = f.tell()
        f.seek(size, 1)
        s = f.readline() # skip forward to next line ending
        yield start, f.tell() - start
        if not s:
            break

line_count = 0
count = 0



def process(file,chunk):
	global line_count,count
	f = open(file, "rb")
	f.seek(chunk[0])
	s = f.read(chunk[1])
	# print s
	for line in s.split(os.linesep):
		# continue
		line_count += 1
		parts = line.split()

		if len(parts) <= 0:
			if len(line) != 0 or line is None:
				print "Part is not suitable='%s'" % line 
			continue
		# time.sleep(1)
		if not (parts[3] == "4069"):
			# output.write(os.linesep)
			continue
		count += 1
		# continue
		agent = " ".join(parts[14:-1])
		# parsed_ua = uas_parser.parse(agent,entire_url='ua_icon,os_icon')
		# parsed_ua = parsed_ua.get('os_family',"")
		parsed_ua = ""#"-".join(httpagentparser.simple_detect(agent))
		# print parsed_ua
		# exit()
		hashed_agent = hashlib.md5(agent).hexdigest()
		# agents[hashed_agent] = agent
		other = parts[:14]
		
		output.write(" ".join( [parts[0], parsed_ua ,parts[2], hashed_agent,os.linesep] ))


for chunk in getchunks(FILE):
	process(FILE,chunk)
	
print "Line Count %d vs %d" % (line_count,count)