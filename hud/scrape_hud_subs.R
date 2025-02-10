# picture of subsidized housing
library(dplyr)
library(purrr)
library(rvest)

base_url <- "https://www.huduser.gov/portal/datasets/assthsg.html#data_2009-2023"

# need fragment to get it to start with accordions open
set_path <- function(ref, base) {
    urltools::fragment(base) <- NULL
    urltools::path(base) <- ref
    base
}

# some urls have more than 1 year to show what vintage boundaries are used, but 1st year is safe
urls <- tibble(href = read_html(base_url) |>
    html_elements(xpath = "//ul/li//a[contains(@href,'xlsx')]") |>
    html_attr("href")) |>
    mutate(url = map_chr(href, \(x) set_path(x, base_url))) |>
    mutate(year = stringr::str_extract(url, "\\d{4}")) |>
    mutate(fn = basename(url)) |>
    select(year, fn, url)

base_dir <- file.path("hud", "picture_subs")
if (!dir.exists(base_dir)) {
    dir.create(base_dir)
}
pwalk(urls, function(year, fn, url) {
    yr_dir <- file.path(base_dir, year)
    if (!dir.exists(yr_dir)) {
        dir.create(yr_dir)
    }
    path <- file.path(yr_dir, fn)
    if (!file.exists(path)) {
        download.file(url, path)
        cli::cli_alert_info("{path} written")
    }
})

readr::write_csv(urls, file.path(base_dir, "picture_of_subs_housing_urls.csv"))

