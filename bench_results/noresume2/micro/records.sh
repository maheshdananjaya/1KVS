#!/bin/sh
#This is the script to extract data from crash recovery files. 


file_name="result_all_threads_16x8.txt"
new_file_name="record_${file_name}"
#grep -Fn 'COMPUTE CRASH' result_all_threads_16x8.txt    | head -n1 #multiple fields
line=$(sed -n '/COMPUTE CRASH/=' result_all_threads_16x8.txt  | head -1)
end_line="$(($line + 100))"
start_line="$(($line - 100))"
echo $start_line
echo $end_line
echo $file_name
echo $new_file_name
echo "sed -n "${start_line},${end_line}p" ${file_name} > "${new_file_name}""
sed -n "${start_line},${end_line}p" "$file_name" > "$new_file_name"
