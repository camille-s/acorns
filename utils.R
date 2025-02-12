vec_to_patt <- function(x) {
    x <- paste(x, collapse = "|")
    x <- sprintf("(%s)", x)
    x
}