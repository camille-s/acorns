# recent years of SIPP when it became annual & has consistent formatting
# standard ftp-esque directory structure
library(dplyr)
library(purrr)
library(rvest)
library(future)
source("utils.R")

plan(multisession)

base_url <- "https://www2.census.gov/programs-surveys/sipp/data/datasets"

# skip sas & spss but might as well grab everything else
urls <- tibble(href = read_cb_dir(base_url)) |>
    mutate(year = stringr::str_extract(href, "^\\d{4}") |> as.numeric()) |>
    filter(!is.na(year), year >= 2018) |>
    mutate(year = as.character(year)) |>
    mutate(url = file.path(base_url, href)) |>
    mutate(node = map(url, read_cb_dir)) |>
    tidyr::unnest(node) |>
    mutate(ext = stringr::str_extract(tolower(node), "(?<=\\.).+$")) |>
    filter(
        !grepl("^(dta|sas)", ext),
        !is.na(ext),
        !grepl("(dta.zip|sasdata.zip|sas.zip)", node)
    ) |>
    rename(dir = url) |>
    mutate(url = file.path(dir, node))

urls |>
    select(year, url, node) |>
    furrr::future_pwalk(function(year, url, node) {
        year_dir <- mkdir_if_none(file.path("sipp", year))
        path <- file.path(year_dir, node)
        if (!file.exists(path)) {
            res <- safe_curl(url, path)
            print_errors(path, res)
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)
