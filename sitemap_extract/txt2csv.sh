
#!/usr/bin/sh

# Loop through all .txt files in the current directory 
# add source filename and output to .csv
for file in *.txt; do
    awk -v fname="${file%.txt}" 'NR > 1 {print fname "," $0}' "$file"
done > combined.csv
