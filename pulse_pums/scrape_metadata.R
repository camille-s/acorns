# scrape list of releases for download
# site structure is changed to have tabs for each year
# hierarchy:
# * h3 gives phase number
# * h4 gives dates within each phase
# * a gives link to csv
library(rvest)
library(purrr)
library(dplyr, warn.conflicts = FALSE)

if (exists("snakemake")) {
    output_path <- snakemake@output[["meta"]]
} else {
    output_path <- "datasets_meta.csv"
}

check_prev <- function(file) {
    if (file.exists(file)) {
        df <- read.csv(file)
        nrow(df)
    } else {
        NULL
    }
}

make_https <- function(x) {
    urltools::scheme(x) <- "https"
    x
}

get_a_attrs <- function(el) {
    elem_type <- rvest::html_name(el)
    if (elem_type == "a") {
        href <- stringr::str_remove(rvest::html_attr(el, "href"), "^//")
    } else {
        href <- NULL
    }
    txt <- rvest::html_text2(el)
    dplyr::tibble(elem = elem_type, href = href, txt = txt)
}

cb_base <- "https://www.census.gov/programs-surveys/household-pulse-survey/data/datasets.html"
# get years of data from tablist
years <- read_html(cb_base) |>
    html_elements(xpath = '//ul[@role="menu"]//a') |>
    map_dfr(\(x) list(year = html_text(x), yr_url = html_attr(x, "href"))) |>
    mutate(yr_url = stringr::str_remove(yr_url, "\\#.+$"))

# why tf did they change to ul but keep previous waves as h4
# also one cycle has the dates in a different place?
# last round in october 2024 is totally different
nodes <- years |>
    mutate(nodes = map(yr_url, read_html)) |>
    mutate(nodes = map(nodes,
        html_elements,
        xpath = "//h3[contains(text(), 'PUF')] | //h4 | //a[contains(@href, 'HPS')]"
    )) |>
    mutate(nodes = map(nodes, \(x) map_dfr(x, get_a_attrs))) |>
    tidyr::unnest(nodes)

make_date <- function(x) {
    x <- paste(1, x)
    x <- lubridate::dmy(x)
    x <- strftime(x, "%b%Y")
    x <- tolower(x)
    x
}
collapse_regex <- function(x) {
    x <- paste(x, collapse = "|")
    sprintf("(%s)", x)
}

hhp_meta <- nodes |>
    select(-yr_url) |>
    mutate(phase = ifelse(elem == "h3", txt, NA_character_) |>
        stringr::str_extract("(Phase [\\d\\.]+|\\w+ \\d{4}(?= PUF))")) |>
    mutate(dates = ifelse((elem == "h4") | (elem == "h3" & grepl("\\d{4}", phase)),
        txt,
        NA_character_
    ) |>
        stringr::str_remove("^Household Pulse Survey PUF: ") |>
        stringr::str_replace_all(", ", " ") |>
        stringr::str_replace_all("[[:punct:]]", "_")) |> # delimeter sometimes a hyphen, sometimes en-dash
    mutate(week = stringr::str_extract(txt, "(Week|Cycle) (\\d+)")) |>
    mutate(week_type = tolower(stringr::str_extract(week, "(Week|Cycle)"))) |>
    mutate(week_num = readr::parse_number(week)) |>
    mutate(week = stringr::str_c(week_type, stringr::str_pad(week_num, 2, "left", "0"), sep = "")) |>
    tidyr::fill(phase, week, .direction = "down") |>
    # fill in october 2024 week
    # mutate(week = ifelse(is.na(week),
    #                         stringr::str_replace_all(tolower(phase), "\\s", "_"),
    #                         week)) |>
    mutate(week = case_when(
        !is.na(week) ~ week,
        stringr::str_detect(phase, collapse_regex(month.name)) ~ make_date(phase),
        TRUE ~ stringr::str_replace_all(tolower(phase), "\\s", "_")
    )) |>
    group_by(week) |>
    tidyr::fill(dates, .direction = "downup") |>
    ungroup() |>
    mutate(wave = floor(readr::parse_number(phase))) |>
    filter(elem == "a") |>
    mutate(file_type = stringr::str_extract(txt, "(SAS|CSV)")) |>
    filter(
        wave > 1,
        file_type == "CSV"
    ) |>
    mutate(year = stringr::str_extract(href, "\\d{4}")) |>
    mutate(year = ifelse(grepl("December", dates), NA_character_, year)) |>
    arrange(wave, week_num) |>
    mutate(href = map_chr(href, make_https)) |>
    tidyr::separate(dates, into = c("start", "end"), sep = " _ ") |>
    mutate(across(start:end, list(yr = \(x) stringr::str_extract(x, "\\d{4}")))) |>
    mutate(across(start_yr:end_yr, \(x) coalesce(x, year))) |>
    tidyr::fill(start_yr, end_yr) |>
    select(wave, phase, week, start_yr, end_yr, start_date = start, end_date = end, url = href) |>
    tidyr::pivot_longer(matches("^(start|end)"), names_to = c("endpt", ".value"), names_sep = "_") |>
    mutate(date = stringr::str_remove(date, "\\d{4}") |>
        paste(yr) |>
        lubridate::mdy()) |>
    tidyr::pivot_wider(id_cols = c(wave, phase, week, url), names_from = endpt, values_from = date) |>
    # lol did so much work to get october 2024 set but it's only national
    filter(grepl("(week|cycle)", week))

if (any(duplicated(hhp_meta$start)) | any(duplicated(hhp_meta$end))) {
    stop("duplicates in meta dates")
}

# if the file already exists, does the new data frame have more rows than in the prev file?
# if so, overwrite
# else, don't write anything
prev <- check_prev(output_path)
if (is.null(prev) || (is.numeric(prev) & (nrow(hhp_meta) > prev))) {
    should_write <- TRUE
} else {
    should_write <- FALSE
}

if (should_write) {
    write.table(hhp_meta, output_path, sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)
}

