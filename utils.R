vec_to_patt <- function(x) {
    x <- paste(x, collapse = "|")
    x <- sprintf("(%s)", x)
    x
}

# get vector of href attributes based on xpath
# cleaner way to make a data frame column
get_href <- function(nodes, xpath) {
    els <- rvest::html_elements(nodes, xpath = xpath)
    rvest::html_attr(els, "href")
}

# census bureau directories have nice standard formatting
read_cb_dir <- function(url, xpath = "//table//tr//a", incl_text = FALSE) {
    html <- rvest::read_html(url)
    nodes <- rvest::html_elements(html, xpath = xpath)
    # keep elements after parent directory link
    text <- rvest::html_text2(nodes)
    href <- rvest::html_attr(nodes, "href")
    start_idx <- grep("Parent Directory", text) + 1
    idx <- start_idx:length(text)
    if (incl_text) {
        list(
            href = href[idx],
            text = text[idx]
        )
    } else {
        href[idx]
    }
}

# mimic bash mkdir -p x
mkdir_if_none <- function(dir) {
    if (!dir.exists(dir)) {
        dir.create(dir)
    }
    dir
}

# returns list: result = download path, error if any
safe_curl <- purrr::safely(curl::curl_download, NULL)

print_errors <- function(path, result) {
    if (!is.null(res$error)) {
        cli::cli_alert_danger("{path} failed: {res$error}")
    } else {
        cli::cli_alert_success("{path} written")
    }
}