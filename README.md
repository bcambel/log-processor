
Unzip and Map-Reduce Log Files
===================

MongoDb backed Log file processor for zipped huge log files.
Uses MultiProcessing to create processes to execute the unzipping and map reduce operation.
Unzip operation uses multiprocessing.Pool to parallelize the tasks.

Installation
===================

`git clone https://github.com/bcambel/log-processor.git`

Best way to manage Python and third party libraries is using virtualenv - http://www.virtualenv.org/en/latest/#installation

If pip is not installed before virtualenv, download lib and run the command `python virtualenv.py`

`virtualenv venv -p /usr/local/bin/python2.7 --distribute`

Virtual Env will automatically install pip to manage the dependencies. Use requirements.txt file to install dependencies

After installation complete run the command `source venv/bin/activate`. This command will activate the virtualenv in your terminal screen.

The only real dependency is `PyMongo` `pip install pymongo` will install pymongo and refer it to the project.


The most important files are "src/zip_task_manager.py", "src/file_task_monitor" -> "src/file_task_worker"

- Basic Usage `./run.bash <directory>` script to execute the script

Note that run.bash already activates the virtualenv (line 14)

Important things
=============================
You can also run the zipper and the log monitor seperately. Both scripts run continuously. Log Monitor has a multiprocessing.Pool to
map-reduce log files. Each file will have its own process to execute the job. MongoDb contains information about the currently execution jobs
and their statuses.

Parts
-----------
Use `src/zip_task_manager.py --input-dir=<directory>`
to convert Unzip files into log files

Use `src/file_task_monitor.py --input-dir=<directory>`
to map reduce log files

Use `args.py` or `./run.sh -h`
to print help



