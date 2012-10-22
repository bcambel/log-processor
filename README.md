
Unzip and Map-Reduce Log Files
===================



Installation
===================

git clone https://github.com/bcambel/log-processor.git

Best way to manage Python and third party libraries is using virtualenv - http://www.virtualenv.org/en/latest/#installation

If pip is not installed before virtualenv, download lib and run the command `python virtualenv.py`

`virtualenv venv -p /usr/local/bin/python2.7 --distribute`

Virtual Env will automatically install pip to manage the dependencies. Use requirements.txt file to install dependencies

After installation complete run the command `source venv/bin/activate`. This command will activate the virtualenv

The most important files are "src/zip_task_manager.py", "src/file_task_monitor" -> "src/file_task_worker"

- Basic Usage
	`./run.sh <directory>`
script to execute the script

Important things
=============================
- Parts
Use `src/start.py --input-dir=<directory>`
to convert Unzip files into log files

Use `src/file_task_monitor.py --input-dir=<directory>`
to map reduce log files

Use `args.py` or `./run.sh -h`
to print help



