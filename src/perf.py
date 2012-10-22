import time
from datetime import datetime
from args import parse_arguments
import os
import re

def get_data_from_line(line,index_parts):
	parts = line.split()
	time_info = " ".join([parts[n] for n in index_parts])

	return time_info

def parse_date(date):
	return datetime.strptime(date,"%Y-%m-%d %H:%M:%S.%f")

def search_info_files():
	info_files = [f for f in os.listdir(args.input_dir) if re.match(r'.+info.log',f)]

	results = []
	for file in info_files:
		with open(args.input_dir+file, "r") as log_stats:
			lines = log_stats.readlines()

		fname = file.split(".")[0]

		start_time = parse_date(get_data_from_line(lines[0],[1,2]))
		end_time = parse_date(get_data_from_line(lines[1],[1,2]))
		duration = get_data_from_line(lines[2],[1])
		line_count = get_data_from_line(lines[3],[1])
		reduced_line = get_data_from_line(lines[4],[1])

		results.append( (fname,start_time,end_time,duration,line_count,reduced_line) )

	return results

if __name__ == "__main__":
	args = parse_arguments()

	while True:
		results = search_info_files()
		print "===================="
		min_date = datetime.max
		min_date_item = ""
		max_date_item = ""
		max_date = datetime.min

		for res in results:
			if res[1] < min_date:
				min_date = res[1]
				min_date_item = res[0]
			if res[2] > max_date:
				max_date = res[2]
				max_date_item= res[0]


			print "%s %s %s %s %s vs %s" % res


		print "-"*10

		print "First log(%s) started: (%s)" %( min_date_item , str(min_date) )
		print "Last log(%s) finished: (%s)" %( max_date_item, str(max_date) )

		print "Done in %s" % ( str(max_date - min_date) )

		time.sleep(3)



