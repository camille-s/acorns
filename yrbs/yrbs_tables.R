# previously downloaded files to analyze because tables had been taken down
# tables are still missing from the data portal but the little app is back online, so snatching those
# just kinda messy to stick together and api is undocumented
library(dplyr)
library(purrr)
library(httr2)
source("utils.R")

base_url <- "https://yrbs-explorer.services.cdc.gov/api/"
survey <- 2
# store temp responses
resp_dir <- mkdir_if_none(file.path("yrbs", "resps"))

# years are a list per survey
# can't figure out what's up with SurveyId, just using 2
cli::cli_h1("querying years")
years <- request(base_url) |>
    req_url_path_append("Years", survey) |>
    req_perform() |>
    resp_body_json() |>
    pluck(1, "Years") |>
    unlist()

perform_batch <- function(reqs, paths = NULL, cap = 30, fill = 60, on_error = "continue", verb = 0) {
    # httr2::local_verbosity(verbosity)
    reqs <- purrr::map(reqs, function(r) {
        r <- httr2::req_user_agent(r, "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0")
        r <- httr2::req_throttle(r, capacity = cap, fill_time_s = fill)
        r
    })

    httr2::with_verbosity(httr2::req_perform_parallel(reqs, paths = paths, on_error = on_error), verbosity = verb) # nolint: line_length_linter.
}

prep_skips <- function(df, col_name, prefix = "", dir = resp_dir) {
    id <- paste0(prefix, df[[col_name]])
    df$path <- file.path(dir, xfun::with_ext(id, "json"))
    df$to_req <- ifelse(file.exists(df$path), "skip", "req")
    split(df, df$to_req)
}
is_success <- function(resp) {
    length(resp[["status_code"]]) > 0 && resp[["status_code"]] == 200
}

safe_read_json <- purrr::possibly(jsonlite::read_json)

# questions are nested under topics
# list of years doesn't return anything to differentiate between years,
# so iterate over them to get each year's questions
cli::cli_h1("querying questions")
qs_base <- request(base_url) |>
    req_url_path_append("Questions") |>
    req_url_query(SurveyId = survey, ListofLocations = "XX")
questions <- tibble(year = as.numeric(years)) |>
    mutate(req = map(year, \(x) req_url_query(qs_base, ListOfYears = x))) |>
    mutate(resp = perform_batch(req)) |>
    mutate(resp = map(resp, resp_body_json)) |>
    mutate(resp = map(resp, pluck, 1, "Topics")) |>
    mutate(resp = map_depth(resp, 2, as_tibble)) |>
    mutate(resp = map_depth(resp, 2, bind_rows)) |>
    mutate(resp = map_depth(resp, 2, tidyr::unnest_wider, TopicQuestions)) |>
    mutate(resp = map(resp, bind_rows)) |>
    tidyr::unnest(resp) |>
    select(-req)

# question meta: MMWRQuestions
cli::cli_h1("querying mmwr")
mmwr_base <- request(base_url) |>
    req_url_path_append("MMWRQuestions")
mmwr <- questions |>
    distinct(QuestionCode) |>
    mutate(req = map(QuestionCode, \(x) req_url_query(mmwr_base, QuestionId = x))) |>
    mutate(resp = perform_batch(req)) |>
    mutate(resp = map(resp, resp_body_json)) |>
    mutate(resp = map(resp, pluck, 1)) |>
    tidyr::unnest_wider(resp) |>
    select(QuestionCode, Question, ShortQuestionText)

# if a question has a footnote, there's an asterisk in its text
cli::cli_h1("querying footnotes")
foot_base <- request(base_url) |>
    req_url_path_append("FootnotesData") |>
    req_url_query(LocationId = "XX", ViewType = "T")
foot <- questions |>
    distinct(QuestionCode) |>
    semi_join(
        filter(mmwr, grepl("\\*", Question)),
        by = "QuestionCode"
    ) |>
    mutate(req = map(QuestionCode, \(x) req_url_query(foot_base, QuestionId = x))) |>
    mutate(resp = perform_batch(req)) |>
    mutate(resp = map(resp, resp_body_json)) |>
    mutate(resp = map(resp, keep, \(x) x$FootnoteSymbol == "*")) |>
    mutate(resp = map_depth(resp, 2, \(x) x[c("FootnoteSymbol", "FootnoteText")])) |>
    mutate(resp = map(resp, unique)) |>
    tidyr::unnest(resp) |>
    tidyr::unnest_wider(resp) |>
    select(QuestionCode, FootnoteText)

cli::cli_h1("querying data")
data_base <- request(base_url) |>
    req_url_path_append("TableData") |>
    req_url_query(LocationId = "XX")

n_yrs <- 5
recent <- as.numeric(sort(years, decreasing = TRUE))[1:n_yrs]
data_to_do <- questions |>
    filter(year %in% recent) |>
    select(year, QuestionCode) |>
    # slice(10:11) |>
    mutate(req = map2(year, QuestionCode, function(yr, q) {
        req_url_query(data_base, QuestionId = q, Yr = yr)
    })) |>
    mutate(id = paste(year, QuestionCode, sep = "_")) |>
    prep_skips(col_name = "id", prefix = "table-")

Sys.sleep(60)

cli::cli_h1("prepping data")
data <- list()
tictoc::tic()
if (!is.null(data_to_do[["req"]])) {
    n <- nrow(data_to_do[["req"]])
    cli::cli_alert("data requests: {n}")
    data[["req"]] <- data_to_do[["req"]] |>
        mutate(resp = perform_batch(req, path, cap = 10, verb = 3)) |>
        filter(map_lgl(resp, is_success)) |>
        mutate(resp = map(resp, resp_body_json))
}
if (!is.null(data_to_do[["skip"]])) {
    n <- nrow(data_to_do[["skip"]])
    cli::cli_alert("data skips: {n}")
    data[["skip"]] <- data_to_do[["skip"]] |>
        mutate(resp = map(path, safe_read_json)) |>
        filter(lengths(resp) > 0)
}
data <- bind_rows(data) |>
    mutate(resp = map_depth(resp, 2, as_tibble)) |>
    mutate(resp = map(resp, bind_rows)) |>
    select(-req)
tictoc::toc()

write_to_db <- function(con, schema, name, df, overwrite = FALSE) {
    table <- DBI::Id(schema, name)
    if (!DBI::dbExistsTable(con, table) | overwrite) {
        DBI::dbWriteTable(con, table, df, overwrite = overwrite)
        cli::cli_alert_info("{schema}.{name} written to database")
    }
}
# load stuff into duckdb: table of questions, table of mmwr metadata, table of footnotes
con <- DBI::dbConnect(duckdb::duckdb(), "yrbs/yrbstbls.duckdb")
# metadata
DBI::dbSendQuery(con, "CREATE SCHEMA IF NOT EXISTS meta;")
list(questions = questions, meta = mmwr, footnotes = foot) |>
    iwalk(function(df, name) {
        write_to_db(con, schema = "meta", name = name, df = df, overwrite = TRUE)
    })

# data tables per year-question
data |>
    select(year, QuestionCode, resp) |>
    pwalk(function(year, QuestionCode, resp) {
        name <- paste(QuestionCode, year, sep = "_")
        df <- resp |>
            mutate(across(c(MainValue, LowCI, HighCI), readr::parse_number)) |>
            select(-Yr)
        write_to_db(con, schema = "main", name = name, df = df, overwrite = FALSE)
    })

cli::cli_h1("database check")
DBI::dbGetQuery(
    con,
    "SELECT table_schema, COUNT(*) FROM information_schema.tables GROUP BY table_schema;"
)

DBI::dbDisconnect(con)
