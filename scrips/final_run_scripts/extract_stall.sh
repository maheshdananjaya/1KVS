#!/bin/sh
#This is the script to extract data from crash recovery files. 

path_name="bench_results"
paths='stalls'
benches='micro-1000   micro-100000  micro-1M micro-10000'
for path in ${paths}
do
     for bench in ${benches}
     do
        file="result_all_threads_4x8.txt"
        full_path="${path_name}/${path}/${bench}"
        file_name="${full_path}/${file}"
        new_file_name="${full_path}/extracted_${file}"
        #grep -Fn 'COMPUTE CRASH' result_all_threads_16x8.txt    | head -n1 #multiple fields
        line=$(sed -n '/COMPUTE CRASH/=' "${file_name}"  | head -1)
        end_line="$(($line + 10000))"
        start_line="$(($line - 100))"
        echo $start_line
        echo $end_line
        echo $file_name
        echo $new_file_name
        echo "sed -n "${start_line},${end_line}p" ${file_name} > "${new_file_name}""

        sed -n "${start_line},${end_line}p" "$file_name" > "$new_file_name"
     done
done





path_name="bench_results"
paths='stalls'
benches='micro-norecovery-10000   micro-norecovery-1M  micro-norecovery-1000  micro-norecovery-100000'
for path in ${paths}
do
     for bench in ${benches}
     do
        file="result_all_threads.txt"
        full_path="${path_name}/${path}/${bench}"
        file_name="${full_path}/${file}"
        new_file_name="${full_path}/extracted_${file}"
        #grep -Fn 'COMPUTE CRASH' result_all_threads_16x8.txt    | head -n1 #multiple fields
        line=$(sed -n '/COMPUTE	CRASH/=' "${file_name}"  | head -1)
        end_line="$(($line + 10000))"
        start_line="$(($line - 100))"
        echo $start_line
        echo $end_line
        echo $file_name
        echo $new_file_name
        echo "sed -n "${start_line},${end_line}p" ${file_name} > "${new_file_name}""

        sed -n "${start_line},${end_line}p" "$file_name" > "$new_file_name"
     done
done



