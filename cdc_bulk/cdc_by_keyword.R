# socrata portal, easy access
source("utils.R")
library(dplyr)
library(future)

plan(multisession(workers = 14))

base_url <- "https://data.cdc.gov"

urls <- RSocrata::ls.socrata(base_url) |>
    as_tibble() |>
    tidyr::unnest_wider(publisher, names_sep = "_") |>
    janitor::clean_names() |>
    mutate(id = basename(identifier)) |>
    mutate(url = file.path(base_url, "resource", id) |> xfun::with_ext("csv")) |>
    select(url, id, pub = publisher_name, keyword, title)

get_url <- function(x) {
    q <- list(
        "$limit" = "100000000"
    )
    resp <- httr::GET(x, query = q)
    httr::content(resp, show_col_types = FALSE)
}
safe_get_url <- purrr::possibly(get_url, otherwise = NULL)

keywords <- urls |>
    tidyr::unnest(keyword) |>
    mutate(keyword = stringr::str_squish(keyword) |>
        stringr::str_replace_all("\\-", " ")) |>
    filter(grepl("[a-z]", keyword), !is.na(keyword))

# sort(unique(keywords$keyword))

keeps <- vec_to_patt(c(
    "access",
    "adolescent",
    "african",
    "air pollution",
    "air quality",
    "native",
    "asian",
    "autism",
    "reproductive",
    "black",
    "hispanic",
    "clean air",
    "birth control",
    "brfss",
    "central and south american",
    "cultural",
    "discrimination",
    "emotional",
    "disparities",
    "environmental",
    "ethnicity",
    "food security",
    "food insecurity",
    "gender",
    "government order",
    "food workers",
    "hiv",
    "hispanic",
    "high school",
    "housing",
    "incarceration",
    "intimate partner violence",
    "indigenous",
    "language",
    "medicaid",
    "mexican",
    "middle school",
    "\\brace",
    "nhis",
    "nonmarital",
    "pepfar",
    "particulate",
    "pm\\b",
    "poverty",
    "long covid",
    "reproductive",
    "puerto rican",
    "racism",
    "sdoh",
    "sex",
    "social emotional",
    "students",
    "suicide",
    "teen",
    "social isolation",
    "social vulnerability",
    "teen birth",
    "unaccompanied children",
    "vaccine hesitancy",
    "unmet need",
    "white",
    "youth",
    "yrbss"
))

matches <- keywords |>
    filter(grepl(keeps, keyword) | grepl(keeps, tolower(title)))

urls_sub <- urls |>
    semi_join(matches, by = "url") |>
    mutate(pub = stringr::str_remove(pub, "[\\-/].+$")) |>
    mutate(pub = stringr::str_replace_all(pub, c(
        "Centers for Disease Control.+$" = "CDC",
        "HHS.+$" = "HHS",
        "^(.+)\\(([A-Z]+)\\)" = "\\2"
    ))) |>
    mutate(pub = stringr::str_squish(pub)) |>
    mutate(keyword = purrr::map_chr(keyword, toString)) |>
    mutate(title = tolower(title)) |>
    filter(!grepl("daily census tract.level ozone", title)) |> # i think these are the ones crashing 
    filter(!grepl("^places", title)) # skip places, already got it

# clean up publishers to put into directories
paths <- urls_sub |>
    mutate(pub_dir = file.path("cdc_bulk", snakecase::to_snake_case(pub))) |>
    mutate(fn = title |>
        substr(1, 100) |>
        snakecase::to_snake_case() |>
        xfun::with_ext("rds")) |>
    mutate(path = file.path(pub_dir, fn)) |>
    mutate(url = paste(url, "$limit=100000000", sep = "?")) |>
    select(pub_dir, url, path)

paths |>
    furrr::future_pmap(function(pub_dir, url, path) {
        if (!dir.exists(pub_dir)) {
            dir.create(pub_dir)
        }
        if (!file.exists(path)) {
            df <- safe_get_url(url)
            if (is.null(df)) {
                cli::cli_alert_danger("{path} failed")
            } else {
                saveRDS(df, path)
                cli::cli_alert_success("{path} written")
            }
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

# some of these were crashing before so trying curl. tbh this should be a bash script
# res <- curl::multi_download(
#     paths$url, paths$path, progress = TRUE, resume = TRUE
# )

readr::write_csv(urls_sub, "cdc_bulk/cdc_keyword_datasets.csv")

plan(sequential)

