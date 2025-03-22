#!/usr/bin/env bash
# uses swagger API but pagination is a pain
# also have direct download link to a csv---4.7G unpacked so just filter for a state & build db
stateabbr=$1

wd=$(pwd)
dir="cfpb/complaints"
mkdir -p "$dir" && cd "$dir" || exit 1
echo "Moving to $dir"

url="https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
zipfile=$(basename "$url")
csvfile=$(basename "$zipfile" .zip)
dbfile="complaints.duckdb"
dictfile="complaint_dict.csv"

if [ ! -f "$zipfile" ]; then 
    curl -o "$zipfile" "$url"
fi

if [ ! -f "$csvfile" ]; then
    unzip "$zipfile"
fi

# make stateabbr lowercase
table=$(echo "$stateabbr" | tr '[:upper:]' '[:lower:]')

duckdb -c "
CREATE OR REPLACE TABLE '$table' AS
  SELECT *
    FROM read_csv(
      '$csvfile',
      strict_mode = false,
      normalize_names = true,
      nullstr = ['', 'N/A'],
      ignore_errors = true,
      types = { 'date_sent_to_company': 'VARCHAR' }
    ) 
    WHERE state = '$stateabbr';
" "$dbfile"
# create table from result of prepared query
# duckdb -c "EXECUTE query_state('$stateabbr');"

# duckdb -c "
# CREATE OR REPLACE TABLE '$table' AS 
#   FROM (EXECUTE query_state($stateabbr)); 
# "
duckdb -c "
SELECT state, COUNT(*)
  FROM '$table'
  GROUP BY state;
" "$dbfile"

# scraping dictionary works easier in R than I can get with xidel & sed
Rscript ../scrape_complaint_dict.R "$dictfile"

duckdb -c "
CREATE OR REPLACE TABLE dict AS
  SELECT *
    FROM read_csv('$dictfile'); 
" "$dbfile"

duckdb -c "
SELECT *
  FROM dict
  LIMIT 3;
" "$dbfile"

# clean up
rm "$csvfile"

echo "Moving back to $wd"
cd "$wd" || exit 1

