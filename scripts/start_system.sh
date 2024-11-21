#!/bin/bash

if [ "$#" -ne 1 ]; then
    input_no=3
else
    input_no=$1
fi

# Run the script input_no times
for ((i=0; i<input_no; i++))
do
    python3 src/main.py "configs/test_config_${input_no}.json" $i &
done
