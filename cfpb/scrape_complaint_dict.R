args <- commandArgs(trailingOnly = TRUE)
path_out <- args[1]
string_cleanup <- function(x) {
    x <- stringr::str_replace_all(x, "\\s{2,}", ";")
    x <- stringr::str_replace_all(x, "[’‘]", "'")
    x <- stringr::str_replace_all(x, "–", "-")
    x
}
rvest::read_html("https://cfpb.github.io/api/ccdb/fields.html") |>
    rvest::html_table() |>
    purrr::pluck(1) |>
    janitor::clean_names() |>
    dplyr::mutate(dplyr::across(dplyr::everything(), string_cleanup)) |>
    readr::write_csv(path_out)