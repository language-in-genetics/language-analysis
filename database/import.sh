#!/bin/bash

# Loop through all .gz files in current directory
for file in *.gz; do
    echo "Processing $file..."
    # Decompress and pipe directly to psql
    # CSV with a “weird” delimiter so no quoting/escaping kicks in:
    gunzip -c "$file" | psql -v ON_ERROR_STOP=1 -d crossref -c "
    COPY raw_text_data (filesrc)
    FROM STDIN
    WITH (FORMAT csv, DELIMITER E'\x1F', QUOTE E'\x02', ESCAPE E'\x02');"
done
