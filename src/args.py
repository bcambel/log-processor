import argparse
import logging
import sys
from utils import cpu_count
from informant import inform



def parse_arguments(help=False):

	inform("Cpu count is %d" % cpu_count)

	parser = argparse.ArgumentParser(
									 formatter_class=argparse.RawDescriptionHelpFormatter,
									 description="""
Unzip and Map-Reduce Log Files
===================
The most important files are "zip_task_manager.py", "file_task_monitor" -> "file_task_worker"
- Basic Usage
	`./run.sh <directory>`
script to execute the script
=============================
- Parts
Use `src/start.py --input-dir=<directory>`
to convert Unzip files into log files

Use `src/file_task_monitor.py --input-dir=<directory>`
to map reduce log files

Use `args.py` or `./run.sh -h`
to print help
""")

	parser.add_argument("--input-dir",help="Where to read log files",required=True)
	parser.add_argument("--max-proc",help="maximum processor count. Runs as a separate program.",default=cpu_count,type=int)
	parser.add_argument("--flush",help="setup how frequent the read write op will be",type=int,default=10000)

	if help:
		parser.print_help()
		sys.exit(0)
	else:
		args = parser.parse_args()

	for attr, value in args.__dict__.iteritems():
		inform( "Params: %s = (%s) " % (attr, value) )

	if args.max_proc <= 0:
		logging.warn("=====Setting Max_Processor=%d. MaxProc(%d) was invalid" % (cpu_count,args.max_proc))
		args.max_proc = cpu_count
	elif args.max_proc <= 1 and cpu_count > 2:
		logging.warn("=====%d CPU exists. You selected only %d Process to run" % (cpu_count,args.max_proc))
	elif args.max_proc > cpu_count:
		logging.warn("=====%d CPU exist. You selected %d processes to run. Not suggested!!")

	if args.input_dir is None or args.input_dir == "":
		inform("Directory is not SET! Pass Directory as a parameter")
		sys.exit(0)

	if args.input_dir[-1] != "/":
		args.input_dir += "/"


	return args

if __name__ == "__main__":
	parse_arguments(help=True)