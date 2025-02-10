library(dplyr)
library(purrr)
library(arcgislayers)
library(sf)
library(future)

plan(multisession)

safe_arc_open <- possibly(arcgislayers::arc_open, otherwise = NULL)

base_url <- "https://services.arcgis.com/XG15cJAlne2vxtgt/ArcGIS/rest/services"
# arc_open returns data frame with cols name, type, url
all_services <- arcgislayers::arc_open(base_url)[["services"]] |>
    as_tibble() |>
    filter(!grepl("(Guam|Puerto.?Rico|^POST_|.+_NRI)", name)) |>
    filter(type == "FeatureServer") |>
    mutate(url = stringr::str_replace_all(url, " ", "%20")) |>
    mutate(service = furrr::future_map(url, safe_arc_open, .options = furrr::furrr_options(seed = TRUE))) |>
    mutate(desc = map_chr(service, function(x) {
        if (is.null(x)) {
            NA_character_
        } else {
            x[["serviceDescription"]]
        }
    }))

services <- all_services |>
    filter(lengths(service) > 0) |>
    mutate(subset = grepl("(vulnerable|vulnerability|floodplain$|resilience|climrr|justice|brownfield|risk_index|food_access)", tolower(name)) |
        grepl("(EPA|NRI|CJEST|CRCI_FEMA|Digital_Distress)", name) |
        name %in% c(
            "All_Zip_Count", "EPA_Radiation_Air_Monitors",
            "Equity_Vulnerability_Static_Tracts_Health_Places_Index_30_PHASC",
            "Cold_Wave_Hazard", "Extreme_Heat_Hazard", "High_Density_Vulnerable_Places", "High_Flooded_Water_Mark"
        ))


services |>
    select(-service) |>
    readr::write_csv("fema/fema_arc_services.csv")

service_has_layers <- function(service) {
    !is.null(service$layers)
}

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
    mutate(has_layers = map_lgl(service, service_has_layers)) |>
    filter(subset, has_layers) |>
    mutate(path = file.path("fema", name) |> xfun::with_ext("gpkg")) |>
    select(service, path) |>
    furrr::future_pwalk(function(service, path) {
        if (!file.exists(path)) {
            layers <- read_layers(service)
            safe_write(layers, path)
        }
    }, .options = furrr::furrr_options(seed = TRUE), .progress = TRUE)

plan(sequential)