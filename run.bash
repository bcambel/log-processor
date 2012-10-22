#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

input_dir="$1"
max_cpu="$2"
flush="$3"
zip_worker_log="zip_worker.log"
log_worker_log="log_worker.log"

source venv/bin/activate
extra_parameters=""

if [ ${#max_cpu} -ne 0 ]
then
	extra_parameters="--max-proc="$max_cpu
fi

if [ ${#flush} -ne 0 ]
then
 	echo "Process flush"
 	echo "$extra_parameters"
	extra_parameters=$extra_parameters" --flush="$flush
fi

echo $extra_parameters

if [ $# -le 1 ]
then
	die "Use run.sh -h to see help. Invalid number of arguments supplied\nUsage - $0 <file-name> $1 <max_processor> $2 <flush rate>"
fi

if [[ $input_dir == "-h" || $input_dir == "--help" ]]
then
	python src/args.py -h

fi

echo "Input file is "$input_dir

if [[ -d $input_dir ]]
then
	python src/zip_task_manager.py --input-dir=$input_dir $extra_parameters > $zip_worker_log 2>&1 &
	python src/file_task_monitor.py --input-dir=$input_dir $extra_parameters > $log_worker_log 2>&1 &
	tail -f *.log
fi

