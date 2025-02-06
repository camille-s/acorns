# requires an in-house package: remotes::install_github("ct-data-haven/cwi")
library(dplyr)
library(purrr)
library(future)

plan(multisession)

acs <- cwi::acs_vars |>
    mutate(table = stringr::str_remove(name, "_.+$")) |>
    distinct(table, concept)

dec <- cwi::decennial_vars |>
    mutate(table = stringr::str_remove(name, "_.+$")) |>
    distinct(table, concept)

datasets <- tibble::lst(acs, dec)
states <- c(CT = "09", MD = "24")
year <- 2023

imap(states, function(fips, state) {
    imap(datasets, function(data_tbl, data_id) {
        dir <- paste(state, data_id, sep = "_")
        if (!dir.exists(dir)) {
            dir.create(dir)
        }
        furrr::future_map(data_tbl$table, function(tbl) {
            path <- file.path(dir, tbl) |> xfun::with_ext("rds")
            if (!file.exists(path)) {
                if (data_id == "acs") {
                    fun <- cwi::multi_geo_acs
                } else {
                    fun <- cwi::multi_geo_decennial
                }
                df <- fun(
                    table = tbl,
                    # year = year, 
                    state = state, 
                    tracts = "all", 
                    us = TRUE, 
                    sleep = 3,
                    verbose = FALSE
                )
                saveRDS(df, path)
                path

            }
        }, .options = furrr::furrr_options(seed = TRUE))
    })
})

