library(dplyr)
library(purrr)
library(arcgislayers)
library(sf)
library(future)

plan(multisession)

base_url <- "https://services.arcgis.com/XG15cJAlne2vxtgt/ArcGIS/rest/services"
services <- arcgislayers::arc_open(base_url)[["services"]] |>
    as_tibble() |>
    filter(grepl("(vulnerable|vulnerability|floodplain$|climrr|justice|brownfield|risk_index|food_access)", tolower(name)) |
        grepl("(EPA|NRI)", name)) |>
    filter(!grepl("(Guam|Puerto.?Rico|^POST_|.+_NRI)", name)) |>
    filter(type == "FeatureServer") |>
    mutate(service = map(url, arc_open)) |>
    mutate(desc = map_chr(service, "serviceDescription"))

services |>
    select(-service) |>
    readr::write_csv("fema/fema_arc_services.csv")


read_layers <- function(service) {
    layers <- arcgislayers::get_all_layers(service)
    layer_names <- service$layers$name
    layers_sf <- purrr::map(layers$layers, arcgislayers::arc_select)
    layers_sf <- purrr::map(layers_sf, dplyr::select, -dplyr::any_of("FID"))
    layers_sf <- setNames(layers_sf, snakecase::to_snake_case(layer_names))
    layers_sf
}

write_layers <- function(layers_sf, path) {
    purrr::iwalk(layers_sf, function(layer, id) {
        sf::st_write(layer,
            dsn = path,
            layer = id,
            append = FALSE,
            delete_dsn = FALSE,
            delete_layer = TRUE
        )
    })
}

safe_write <- purrr::possibly(write_layers, otherwise = NULL)

services |>
    mutate(path = file.path("fema", name) |> xfun::with_ext("gpkg")) |>
    select(service, path) |>
    furrr::future_pwalk(function(service, path) {
        if (!file.exists(path)) {
            layers <- read_layers(service)
            safe_write(layers, path)
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)