library(dplyr)
library(purrr)
library(rvest)
library(future)

plan(multisession)

base_url <- "https://coastalimagery.blob.core.windows.net/ccap-landcover/CCAP_bulk_download/Regional_30meter_Land_Cover/index.html"

safe_download <- possibly(curl::curl_download, otherwise = NULL)

urls <- tibble(fn = read_html(base_url) |>
    html_elements(xpath = "//a") |>
    html_attr("href") |>
    as.character()) |>
    mutate(type = xfun::file_ext(fn)) |>
    filter(type %in% c("pdf", "xml", "tif"))

urls |>
    mutate(url = file.path(dirname(base_url), fn) |>
        stringr::str_remove("\\s(?=\\.)")) |>
    select(fn, url) |>
    furrr::future_pmap(function(fn, url) {
        if (!file.exists(fn)) {
            safe_download(url, fn)
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)
