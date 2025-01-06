#!/bin/bash

# Check for correct number of arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <base_file_name> <number_of_files>"
    exit 1
fi

base_file_name=$1
num_files=$2
output_file="${base_file_name}_average.txt"

# Temporary files to store intermediate results
temp_c2=$(mktemp)
temp_c3=$(mktemp)

# Initialize temporary files
> "$temp_c2"
> "$temp_c3"

# Process each file and extract columns
for ((i=1; i<=num_files; i++)); do
    file="${base_file_name}_${i}"
    if [[ ! -f "$file" ]]; then
        echo "File $file does not exist!"
        exit 1
    fi

    # Extract the second and third columns and append to temporary files
    awk -F, '{print $2}' "$file" > "temp_c2_$i"
    awk -F, '{print $3}' "$file" > "temp_c3_$i"
done

# Combine columns row-wise and compute averages
paste temp_c2_* > "$temp_c2"
paste temp_c3_* > "$temp_c3"

# Calculate averages and adjust the second column
awk '{
    sum=0; for (i=1; i<=NF; i++) sum+=$i;
    avg=sum/NF;
    if (NR==1) offset=avg;
    adjusted=(avg-offset)/10;
    print adjusted
}' "$temp_c2" > adjusted_c2.txt

awk '{
    sum=0; for (i=1; i<=NF; i++) sum+=$i;
    avg=sum/NF;
    print avg
}' "$temp_c3" > avg_c3.txt

# Combine results into the output file
paste -d',' adjusted_c2.txt avg_c3.txt > "$output_file"

# Cleanup temporary files
rm temp_c2_* temp_c3_* "$temp_c2" "$temp_c3" adjusted_c2.txt avg_c3.txt

echo "Result saved to $output_file"
