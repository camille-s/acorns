library(dplyr)
library(purrr)
library(future)
plan(multisession)

# url <- "https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2021/2021_public_lar_csv.zip"
yrs <- rlang::set_names(2018:2023)
dfs <- map(yrs, \(x) stringr::str_glue("https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/{x}/{x}_public_lar_csv.zip")) |>
    furrr::future_map(function(url) {
        fn <- basename(url)
        path <- file.path("~", "code", "hmda_stash", fn)
        if (!file.exists(path)) {
            curl::curl_download(url, destfile = path, quiet = FALSE)
        }
    })
plan(sequential)
