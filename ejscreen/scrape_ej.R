library(dplyr)
library(purrr)
library(rvest)
library(future)

plan(multisession)

wget::wget_set(method = "wget", extra = "--no-check-certificate")
# ftp package isn't working bc ssl certs are bad
base_url <- "https://gaftp.epa.gov/EJScreen"

list_paths <- function(url) {
    html <- httr::GET(url)
    html <- httr::content(html)
    html <- rvest::html_elements(html, xpath = "//table//a[not(starts-with(@href,'?'))][not(starts-with(@href,'/'))]")
    hrefs <- rvest::html_attr(html, "href")
    paths <- file.path(url, hrefs)
    types <- xfun::file_ext(paths)
    purrr::map2(paths, types, function(p, type) {
        if (type == "") {
            list_paths(p)
        } else {
            p
        }
    })
}


urls <- tibble(url = unlist(list_paths(base_url), recursive = TRUE)) |>
    mutate(year = stringr::str_extract(url, "\\d{4}")) |>
    mutate(type = xfun::file_ext(url)) |>
    filter(
        !grepl("(DoNotUse|_Patch_)", url),
        type %in% c("pdf", "zip", "xlsx", "docx", "csv"),
        !grepl("gdb", url),
        !grepl("State", url)
    )

urls |>
    select(-type) |>
    furrr::future_pmap(function(url, year) {
        wget::wget_set(method = "wget", extra = "--no-check-certificate")

        if (!dir.exists(year)) {
            dir.create(year)
        }
        fn <- basename(url)
        path <- file.path(year, fn)
        if (!file.exists(path)) {
            download.file(url, path)
        }
    }, .options = furrr::furrr_options(seed = TRUE))

plan(sequential)