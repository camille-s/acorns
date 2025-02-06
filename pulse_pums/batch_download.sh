#!/usr/bin/env bash
# get urls from meta file, download, extract
# separate script builds database
# take file path as argument
meta=$1
# get 4th column of meta file as array
# urls=($(tail -n +2 $meta | awk -F, '{print $4}'))
urls=($(awk -F, '{print $4}' $meta))

# get week/cycle number from path
# e.g. https://www2.census.gov/programs-surveys/demo/datasets/hhp/2022/wk48/HPS_Week48_PUF_CSV.zip -> week48
# or https://www2.census.gov/programs-surveys/demo/datasets/hhp/2024/oct/HPS_OCTOBER2024_PUF_CSV.zip -> oct2024

get_week() {
    path=$1
    dirs=$(dirname "$path")
    yr=$(basename $(dirname "$dirs"))
    num=$(basename "$dirs") # e.g. wk48
    # if num has a digit, replace wk and keep
    # else assume it's a month and append year
    if [[ $num =~ [0-9] ]]; then
        num=$(echo "$num" | sed 's/wk/week/')
    else
        num="$num$yr"
    fi
    echo "$num"
}

# download file if not already in downloads folder
# return path
download() {
    url=$1
    base=$(basename "$url")
    file="downloads/$base"
    if [ ! -f $file ]; then
        curl $url -s -L --output $file
    fi
    echo $file
}

# take url, download, pull out week/cycle number
# work in temp directory so it's easier to know which files are for this week
process() {
    url=$1
    zipfile=$(download "$url") # e.g. downloads/HPS_Week14_PUF_CSV.zip
    week=$(get_week "$url") # e.g. week14, cycle03

    tmpdir="downloads/tmp$week"
    mkdir -p "$tmpdir"
    unzip -qq -uj "$zipfile" -d "$tmpdir"
    files=$(find "$tmpdir" -type f)
    # weights file has "repwgt" in name
    wtfile=$(echo "$files" | grep "repwgt")
    # data file doesn't
    datafile=$(echo "$files" | grep ".csv" | grep -v "repwgt")
    echo "$datafile"
    # give unified names and move to downloads folder
    mv "$wtfile" "downloads/pulse_repwgt_puf_$week.csv"
    mv "$datafile" "downloads/pulse_puf_$week.csv"
    # # also move data dictionary xlsx
    mv "$tmpdir"/*.xlsx downloads
    # rm -rf --verbose "$tmpdir" "$zipfile"
}

export -f process get_week download
export urls
mkdir -p downloads

# call process on each url in urls with parallel
parallel -j 14 process ::: "${urls[@]}"
