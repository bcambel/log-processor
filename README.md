
Unzip and Map-Reduce Log Files
===================
The most important files are "src/zip_task_manager.py", "src/file_task_monitor" -> "src/file_task_worker"

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

