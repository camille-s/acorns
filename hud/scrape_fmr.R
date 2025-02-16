library(dplyr)
source("utils.R")
# endpoints:
# * fmr/statedata/CT gets metro & town data
# * il/statedata/CT gets state-level only
# * il/data/_id_ gets individual locations--map over metros
base_url <- "https://www.huduser.gov/hudapi/public"
hdrs <- httr::add_headers(Authorization = paste("Bearer", Sys.getenv("HUD_KEY")))

metros <- httr::GET(file.path(base_url, "fmr/listMetroAreas"), hdrs) |>
    httr::content() |>
    purrr::map(as_tibble) |>
    bind_rows() |>
    filter(grepl("CT", area_name))

params <- list(
    endpt = list(
        fmr_sub = "fmr/statedata/CT",
        il_state = "il/statedata/CT",
        il_sub = paste("il/data", metros$cbsa_code, sep = "/")
    ),
    year = 2000:2025
)
param_grid <- tidyr::expand_grid(!!!params) |>
    mutate(dataset = names(endpt)) |>
    tidyr::unnest(endpt) |>
    relocate(dataset, .before = 0)

# fmr has differing lengths: one list for metros, one for towns
unnest_fmr <- function(x) {
    x <- x[c("metroareas", "counties")]
    x <- purrr::map_depth(x, 2, as_tibble)
    x <- purrr::map(x, bind_rows)
    x[["metroareas"]] <- dplyr::rename(x[["metroareas"]], name = metro_name)
    x[["counties"]] <- dplyr::rename(x[["counties"]], name = town_name)
    x <- dplyr::bind_rows(x)
    x <- dplyr::select(x, -dplyr::matches("code$"))
    x <- janitor::clean_names(x)
    x
}

unnest_il <- function(x) {
    x <- purrr::modify_at(
        x, grepl("low$", names(x)),
        tibble::enframe,
        name = "hh_size"
    )
    x <- dplyr::as_tibble(x)
    x <- tidyr::unnest_wider(x, dplyr::matches("low"), names_sep = ".")
    x <- dplyr::mutate(x, dplyr::across(dplyr::matches("\\.value"), unlist))
    x <- tidyr::pivot_longer(x, dplyr::matches("low"),
        names_to = c("bracket", ".value"),
        names_sep = "\\."
    )
    x
}

hud_query <- function(url, hdrs) {
    resp <- httr::GET(url, hdrs)
    httr::content(resp)
}
safe_hud_query <- purrr::possibly(hud_query, list())

fetch <- param_grid |>
    # slice(1:20, .by = dataset) |>
    mutate(data = purrr::pmap(list(endpt, year), function(endpt, year) {
        url <- stringr::str_glue("{base_url}/{endpt}?year={year}")
        x <- safe_hud_query(url, hdrs)
        Sys.sleep(1)
        x
    })) |>
    mutate(data = purrr::map(data, "data")) |>
    filter(lengths(data) > 0) |>
    split(~dataset)

intermed <- out <- list()
intermed[["fmr_sub"]] <- fetch[["fmr_sub"]] |>
    mutate(data = purrr::map(data, unnest_fmr)) |>
    mutate(data = purrr::map(data, select, -statename:-metro_name))
intermed[["il_state"]] <- fetch[["il_state"]] |>
    mutate(data = purrr::map(data, unnest_il)) |>
    mutate(data = purrr::map(data, rename, name = statecode)) |>
    mutate(data = purrr::map(data, select, -stateID))
intermed[["il_sub"]] <- fetch[["il_sub"]] |>
    mutate(data = purrr::map(data, unnest_il)) |>
    mutate(data = purrr::map(data, rename, name = area_name)) |>
    mutate(data = purrr::map(data, select, name, median_income:value))
intermed <- intermed |>
    purrr::map(mutate, data = purrr::map(data, select, -any_of("year"))) |>
    purrr::map(tidyr::unnest, data) |>
    purrr::map(mutate, name = stringr::str_remove(name, " FMR Area")) |>
    purrr::map(mutate, name = stringr::str_replace(name, "^CT$", "Connecticut"))

out[["fmr"]] <- intermed[["fmr_sub"]]
out[["il"]] <- bind_rows(intermed[c("il_state", "il_sub")])

purrr::iwalk(out, function(df, id) {
    fn <- stringr::str_glue("hud_{id}_timeseries.rds")
    mkdir_if_none(file.path("hud", id))
    path <- file.path("hud", id, fn)
    saveRDS(df, path)
    print_file_write(path)
})