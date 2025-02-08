library(dplyr)
library(purrr)
library(future)

plan(multisession(workers = 14))

all_states <- unique(tidycensus::fips_codes$state)
states <- c("CT", "MD")

datasets <- list(
    acs = list(year = c(2018, 2023), dataset = "acs5"),
    dec = list(year = 2020, dataset = "dhc")
) |>
    tibble::enframe() |>
    tidyr::unnest_wider(value) |>
    tidyr::unnest(year) |>
    mutate(tbl = map2(year, dataset, tidycensus::load_variables, cache = TRUE)) |>
    mutate(tbl = map(tbl, mutate, table = stringr::str_remove(name, "_.+$"))) |>
    mutate(tbl = map(tbl, filter, !grepl("^PUMA", name))) |>
    mutate(tbl = map(tbl, \(x) unique(x$table))) |>
    mutate(tbl = map(tbl, \(x) stringr::str_subset(x, "PR", negate = TRUE))) |>
    tidyr::expand_grid(state = states) |>
    mutate(data_id = paste(name, dataset, sep = "_")) |>
    select(-dataset) |>
    mutate(base_dir = file.path(data_id, tolower(state), year)) 

# make dirs like acs/ct/2023
nested_mkdir <- function(p) {
    system2("mkdir", c("-p", p))
    invisible(p)
}

map(datasets$base_dir, nested_mkdir)

datasets |>
    tidyr::unnest(tbl) |>
    furrr::future_pwalk(function(name, year, tbl, state, data_id, base_dir) {
        path <- file.path(base_dir, tbl) |> xfun::with_ext("rds")
        if (!file.exists(path)) {
            if (name == "acs") {
                fun <- cwi::multi_geo_acs
            } else if (name == "dec") {
                fun <- cwi::multi_geo_decennial
            } else {
                cli::cli_alert_danger("{path} failed")
                return(NULL)
            }
            df <- fun(
                table = tbl,
                year = year,
                state = state,
                tracts = "all",
                blockgroups = "all",
                us = TRUE,
                sleep = 3,
                verbose = FALSE
            )
            saveRDS(df, path)
            cli::cli_alert_success("{path} written")
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)

