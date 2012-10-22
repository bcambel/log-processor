#!/bin/bash
# SCRIPT:  method1.sh
# PURPOSE: Process a file line by line with PIPED while-read loop.

FILENAME=$1
count=0
while read LINE
do
       let count++
       #echo "$count"
done < $FILENAME

echo -e "\nTotal $count Lines read"