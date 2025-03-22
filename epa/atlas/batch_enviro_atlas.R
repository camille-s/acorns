library(dplyr)
library(purrr)
library(rvest)
library(future)
source("utils.R")

plan(multisession)

# really tired of trying to navigate different directories, just scraping links
base_url <- "https://www.epa.gov/enviroatlas/enviroatlas-data-download-step-2"
base_dir <- file.path("epa", "atlas")

html <- read_html(base_url) 

# 3 tables: 1st is national downloads, 3rd is community downloads
# nat'l: get gdb, metadata
get_tbl_el <- function(el, no_link_first = TRUE) {
    names <- rvest::html_elements(el, css = "thead th")
    names <- rvest::html_text2(names)
    names <- snakecase::to_snake_case(names)

    tbl <- rvest::html_elements(el, css = "tbody tr")
    tbl <- purrr::map(tbl, rvest::html_elements, css = "td")
    tbl <- purrr::map(tbl, function(el) {
        txt <- purrr::map_chr(el, rvest::html_text2)
        a <- purrr::map(el, rvest::html_element, "a")
        href <- purrr::map_chr(a, rvest::html_attr, "href")
        # txt <- rvest::html_text2(el)
        # a <- rvest::html_elements(el, css = "a")
        # href <- rvest::html_attr(a, "href")
        # if (no_link_first) {
        #     href <- c(NA, href)
        # }
        # tibble::lst(txt, href)
        data.frame(value = dplyr::coalesce(href, txt))
    })
    # tbl <- purrr::map(tbl, dplyr::as_tibble)
    # tbl <- purrr::map(tbl, tidyr::unnest, c(txt, href))
    tbl <- purrr::map(tbl, \(x) dplyr::bind_cols(name = names, x))
    tbl <- dplyr::bind_rows(tbl, .id = "row")
    tbl <- tidyr::pivot_wider(tbl, id_cols = row, names_from = name, values_from = value)
    # tbl <- janitor::remove_empty(tbl, "cols")
    tbl <- dplyr::select(tbl, -row)
    tbl
}

html |>
    html_elements(xpath = "//h2[contains(text(),'National Data')]/following-sibling::table[1]") |>
    get_tbl_el() |>
    select(geographic_extent, matches("(esri|metadata)")) |>
    rename(geo = 1, esri = 2, meta = 3) |>
    filter(geo == "Conterminous United States") |>
    tidyr::pivot_longer(-geo, names_to = "type", values_to = "url") |>
    pull(url) |>
    map(function(x) {
        fn <- basename(x)
        geo_dir <- file.path(base_dir, "conus")
        mkdir_if_none(geo_dir)
        path <- file.path(geo_dir, fn)
        if (!file.exists(path)) {
            curl::curl_download(x, path)
        }
    })

city_tbl <- html |>
    html_elements(xpath = "//h2[contains(text(),'Community Data')]/following-sibling::table[1]") |>
    get_tbl_el()

readr::write_csv(city_tbl, here::here("epa/atlas/community_meta.csv"))

city_tbl |>
    select(-csv_with_shp, -changelog) |>
    tidyr::pivot_longer(-community, names_to = "type", values_to = "url") |>
    filter(url != "N/A") |>
    furrr::future_pwalk(function(community, type, url) {
        fn <- basename(url)
        city <- snakecase::to_snake_case(community)
        city_dir <- file.path(base_dir, city)
        mkdir_if_none(city_dir)
        path <- file.path(city_dir, fn)
        if (!file.exists(path)) {
            curl::curl_download(url, path)
            print(url)
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)
