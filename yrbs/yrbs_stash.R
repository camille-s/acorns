library(tidyverse)
library(rvest)
library(future)

plan(multisession)

# base_url <- "https://web.archive.org/web/20241221014411/https://www.cdc.gov/yrbs/files/sadc_2023/MS/SADC_MS_2023_District.zip"
# base_url <- "https://web.archive.org/web/20241221014411/https://www.cdc.gov/yrbs/files/"
comb_url <- "https://web.archive.org/web/20241221014411/https://www.cdc.gov/yrbs/data/index.html"
ann_url <- "https://web.archive.org/web/20241226092400mp_/https://www.cdc.gov/yrbs/data/national-yrbs-datasets-documentation.html"

to_dl <- list()
# national, single db per year
to_dl[["natl"]] <- read_html(comb_url) |>
    html_elements(xpath = "//a[contains(text(),'Access')][contains(@href,'zip')]")

to_dl[["schools"]] <- read_html(comb_url) |>
    html_elements(xpath = "//h4[contains(text(),'Access')]/following-sibling::div//ul/li//a")

to_dl[["annual"]] <- read_html(ann_url) |>
    html_elements(xpath = "//a[contains(text(),'Access')][contains(@href,'zip')]")

safe_curl <- possibly(curl::curl_download, otherwise = NULL)
make_archive <- function(x) {
    domain <- urltools::domain(x)
    archive <- "web.archive.org"
    if (domain != archive) {
        x <- file.path(archive, x)
        urltools::scheme(x) <- "https"
    }
    x
}

imap(to_dl, function(urls, type) {
    if (!dir.exists(type)) {
        dir.create(type)
    }
    urls <- urls |>
        html_attr("href") |>
        as.character()
    url_df <- tibble(url = urls) |>
        mutate(domain = urltools::domain(url)) |>
        mutate(url = map_chr(url, make_archive)) |>
        mutate(fn = basename(url)) |>
        mutate(path = file.path(type, fn)) |>
        select(url, path)
    furrr::future_pwalk(url_df, function(url, path) {
        if (!file.exists(path)) {
            safe_curl(url, path, quiet = FALSE)
        }
    }, .options = furrr::furrr_options(seed = TRUE))
    url_df
})

plan(sequential)
