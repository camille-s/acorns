#!/usr/bin/env bash
# get urls from meta file, download, extract, and copy into db
meta=$1
db=$2
# get 3rd column of meta file as array--week/cycle nums
weeks=($(awk -F, '{print $3}' $meta))
to_trash=$3
echo "$to_trash"

# copy individual data files to db--not parallel so I don't have to worry about concurrent writing
# same but with rep weights. these have scram numbers but not states, so need to filter by scram from week's data table
for week in "${weeks[@]}"; do
    datatbl="${week}" # used to just be a number, now it's e.g. week55, cycle04
    reptbl="${week}_repwgt"
    datacsv="downloads/pulse_puf_$week.csv"
    repcsv="downloads/pulse_repwgt_puf_$week.csv"

    # primary key not implemented in duckdb yet
    duckdb -c "CREATE TABLE IF NOT EXISTS $datatbl AS 
                SELECT * FROM read_csv('$datacsv') 
                WHERE EST_ST = '09';" "$db"
    # duckdb -c "ALTER TABLE $datatbl
    #             ADD PRIMARY KEY (SCRAM);" $db

    duckdb -c "CREATE TABLE IF NOT EXISTS $reptbl AS 
                SELECT * FROM read_csv('$repcsv') 
                SEMI JOIN '$datatbl'
                ON read_csv.SCRAM = $datatbl.SCRAM;" "$db"
    # duckdb -c "ALTER TABLE $reptbl
    #             ADD PRIMARY KEY (SCRAM);" $db
    echo "$week written to $db"

    if [ "$to_trash" == "true" ]; then
        rm --verbose "$datacsv" "$repcsv"
    fi
done

# check how many tables of data or weights
duckdb -c "
    CREATE OR REPLACE VIEW tabletypes AS (
        SELECT table_name, 
            CASE 
                WHEN regexp_matches(table_name, '^(week|cycle)\d+$') THEN 'data'
                WHEN regexp_matches(table_name, '^(week|cycle)\d+_repwgt$') THEN 'rep_weights'
                ELSE 'other'
            END AS tabletype
        FROM information_schema.tables
        WHERE table_schema = 'main'
            AND table_type = 'BASE TABLE'
    );
" $db

duckdb -c "SELECT tabletype, count(*) AS table_count 
    FROM tabletypes
    GROUP BY tabletype;" $db

# create table of metadata--easier access to dates, phase numbers, etc
# verify that these are actually in the schema
duckdb -c "
    CREATE OR REPLACE TABLE metadata AS (
        WITH meta AS (
            SELECT * 
            FROM read_csv('$meta', 
                            header = false, 
                            names = ['phase_num', 'phase', 'week', 'url', 'startdate', 'enddate']))
        SELECT phase_num, phase, week, startdate, enddate
        FROM meta
        WHERE week IN 
            (
                SELECT table_name
                FROM tabletypes
                WHERE tabletype = 'data'
            )
    );
" $db
