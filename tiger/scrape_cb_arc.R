library(tidyverse, quietly = TRUE)
library(arcgislayers)
library(sf)
library(future)

plan(multisession)

base_url <- "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb"
services <- arcgislayers::arc_open(base_url)[["services"]] |>
    mutate(id = str_remove(name, "TIGERweb/")) |>
    mutate(path = file.path(base_url, id, type)) |>
    filter(grepl("(AIAN|Legislative|Places|PUMA|School|State|Census|Current|Tracts|Tribal)", id))
layers <- services |>
    select(service = id, path) |>
    as_tibble() |>
    mutate(layer = furrr::future_pmap(list(service, path), function(service, path) {
        arc_open(path) |>
            get_all_layers()
    }, .options = furrr::furrr_options(seed = TRUE))) |>
    unnest(layer) |>
    unnest(layer) |>
    mutate(type = map_chr(layer, "type")) |>
    mutate(name = map_chr(layer, "name")) |>
    mutate(desc = map_chr(layer, "description")) |>
    mutate(vintage = desc |>
               # found one misspelled month but check for others in full dataset
               str_replace("Janyary", "January") |>
               str_remove("ACS ") |>
               str_extract("\\w+ \\d+, \\d{4}") |>
               lubridate::mdy()) |>
    # to avoid too much nesting skip group layers, assume indiv layers already here
    filter(type != "Group Layer") |>
    distinct(service, name, desc, .keep_all = TRUE) |>
    select(-type)

cli::cli_alert_info(paste(nrow(layers), "layers"))
safe_arc_read <- possibly(arcgislayers::arc_read, otherwise = NULL)

furrr::future_pwalk(layers, function(service, path, layer, name, desc, vintage) {
    service_dir <- snakecase::to_snake_case(service)
    vint_dir <- file.path(service_dir, vintage)
    if (!dir.exists(service_dir)) {
        dir.create(service_dir)
        writeLines(path, file.path(service_dir, "url.txt"))
    }
    if (!dir.exists(vint_dir)) {
        dir.create(vint_dir)
    }
    fn <- snakecase::to_snake_case(layer$name) |> xfun::with_ext("gpkg")
    path_out <- file.path(vint_dir, fn)
    if (!file.exists(path_out)) {
        url <- layer$url
        sf <- safe_arc_read(url)
        if (!is.null(sf)) {
            st_write(sf, path_out, delete_layer = TRUE, quiet = TRUE)   
            cli::cli_alert_success(paste(path_out, "passed"))
        } else {
            cli::cli_alert_danger(paste(path_out, "failed"))
        }
    }
}, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)
