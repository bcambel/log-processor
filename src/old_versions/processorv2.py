#!/usr/bin/env python
import os
import hashlib
from uasparser import UASparser
import httpagentparser
import threading
print "Welcome"

file_name = "/Users/bcambel/Downloads/logs/wac_0394_20121015_0042.log"

output = open("/Users/bcambel/Downloads/logs/wac_0394_20121015_0042.output.log","w+")
agents_file = open("/Users/bcambel/Downloads/logs/agents.log","w+")
uas_parser = UASparser('/Users/bcambel/Downloads/logs/cache')
# agents = {}
line_count = 0
count = 0 
reader_thread = threading.Thread()

with open(file_name,"rb") as myfile:
	myfile.next()
	for line in myfile:
		continue
		line_count = line_count + 1
# > 	line = myfile.readline()
		parts = line.split()
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
		# output.flush()
		# os.fsync(output)
	
# agent_count = 0 
# for k,v in agents.items():
# 	agent_count += 1
# 	agents_file.write(" ".join([k,v,os.linesep]))
	
# print "Agent count is %d" % agent_count
print "Line count is %d vs %d" % (line_count,count)
# for line in myfile.readline():
# 	print line