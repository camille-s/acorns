library(tidyverse)
library(future)

plan(multisession)
base_url <- "https://data.cdc.gov"
meta <- RSocrata::ls.socrata(base_url) |>
    as_tibble() |>
    janitor::clean_names()

urls <- meta |>
    select(modified, bureau_code, title, description, identifier) |>
    filter(grepl("PLACES.+release", title), !grepl("GIS", title)) |>
    mutate(id = basename(identifier)) |>
    mutate(url = file.path(base_url, "resource", id) |> xfun::with_ext("csv")) |>
    unnest(bureau_code) |>
    mutate(level = str_extract(title, "(?<=Health, )([\\w\\s]+)(?= Data \\d{4})") |>
        str_extract("\\w+$") |>
        tolower()) |>
    mutate(year = str_extract(title, "\\d{4}")) |>
    mutate(fn = str_glue("places_{year}_{level}.rds"))

get_url <- function(x) {
    q <- list(
        "$limit" = "100000000"
    )
    resp <- httr::GET(x, query = q)
    httr::content(resp, show_col_types = FALSE)
}
safe_get_url <- possibly(get_url, NULL)

urls |>
    select(level, url, fn) |>
    furrr::future_pmap(function(level, url, fn) {
        if (!dir.exists(level)) {
            dir.create(level)
        }
        path <- file.path(level, fn)
        if (!file.exists(path)) {
            df <- safe_get_url(url)
            if (!is.null(df)) {
                saveRDS(df, path)
            }
        }
        path
    })

plan(sequential)