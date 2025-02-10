# point in time homeless count data
library(dplyr)
library(rvest)

# super straightforward
base_url <- "https://www.huduser.gov/portal/datasets/ahar/2024-ahar-part-1-pit-estimates-of-homelessness-in-the-us.html"

set_path <- function(ref, base) {
    urltools::fragment(base) <- NULL
    urltools::path(base) <- ref
    base
}

a_els <- read_html(base_url) |>
    html_elements(xpath = "//h5[*[contains(text(),'Resource Links')]]//following-sibling::ul//a") 

urls <- tibble(href = html_attr(a_els, "href")) |>
    mutate(url = purrr::map_chr(href, \(x) set_path(x, base_url))) |>
    mutate(fn = basename(url)) |>
    select(fn, url)

base_dir <- file.path("hud", "pit")
if (!dir.exists(base_dir)) {
    dir.create(base_dir)
}

purrr::pwalk(urls, function(fn, url) {
    path <- file.path(base_dir, fn)
    if (!file.exists(path)) {
        download.file(url, path)
        cli::cli_alert_success("{path} written")
    }
})

readr::write_csv(urls, file.path(base_dir, "pit_homeless_urls.csv"))