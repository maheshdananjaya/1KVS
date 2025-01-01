#!/bin/sh
#This is the script to extract data from crash recovery files. 

path_name="../build"
paths='workload'
benches='micro tpcc tatp smallbank'


for path in ${paths}
do
     for bench in ${benches}
     do
        file="result_all_threads.txt"
        full_path="${path_name}/${path}/${bench}"
        file_name="${full_path}/${file}"
        tmp_file="${full_path}/raw_${file}"
        mv ${file_name} ${tmp_file}
        file_name="${full_path}/raw_${file}"
        new_file_name="${full_path}/${file}"
        #grep -Fn 'COMPUTE CRASH' result_all_threads_16x8.txt    | head -n1 #multiple fields
        line=$(sed -n '/COMPUTE CRASH/=' "${file_name}"  | head -1)
        end_line="$(($line + 500))"
        start_line="$(($line - 500))"
        echo $start_line
        echo $end_line
        echo $file_name
        echo $new_file_name
        echo "sed -n "${start_line},${end_line}p" ${file_name} > "${new_file_name}""

        sed -n "${start_line},${end_line}p" "$file_name" > "$new_file_name"
     done
done
