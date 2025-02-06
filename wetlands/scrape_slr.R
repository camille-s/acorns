# yikes they updated this page 15 minutes ago
library(dplyr)
library(purrr)
library(future)
library(rvest)

plan(multisession)

base_url <- "https://coastalimagery.blob.core.windows.net/ccap-landcover/CCAP_bulk_download/Sea_Level_Rise_Wetland_Impacts/index.html"
base_dir <- dirname(base_url)

fns <- read_html(base_url) |>
    html_elements(xpath = "//a[contains(@href,'.zip')]") |>
    html_attr("href")

furrr::future_walk(fns, function(fn) {
    url <- file.path(base_dir, fn)
    if (!file.exists(fn)) {
        curl::curl_download(url, fn)
        print(fn)
    }
})

plan(sequential)