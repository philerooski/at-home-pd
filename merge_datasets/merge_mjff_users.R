library(synapser)
library(tidyverse)

MJFF_PARENT <- "syn18678038"
MJFF_USER_MAPPING <- "syn18678008"

read_syn_csv <- function(syn_id, encoding = "UTF-8") {
  f <- synGet(syn_id)
  df <- read_csv(f$path, locale = locale(encoding = encoding))
  return(df)
}

merge_mjff_users <- function() {
  users <- read_syn_csv(MJFF_USER_MAPPING) %>% 
    select(guid = ancillary_study_identifier, user_id) %>% 
    mutate(user_id = as.character(user_id))
  mjff <- list(
    "syn17098113"= read_syn_csv("syn17098113"),
    "syn17098115" = read_syn_csv("syn17098115"),
    "syn17098116" = read_syn_csv("syn17098116"),
    "syn17098117" = read_syn_csv("syn17098117"),
    "syn17098119" = read_syn_csv("syn17098119"),
    "syn17098120" = read_syn_csv("syn17098120"),
    "syn17098121" = read_syn_csv("syn17098121"),
    "syn17098122" = read_syn_csv("syn17098122"),
    "syn17098123" = read_syn_csv("syn17098123"),
    "syn17098124" = read_syn_csv("syn17098124"),
    "syn17098125" = read_syn_csv("syn17098125"),
    "syn17098126" = read_syn_csv("syn17098126"),
    "syn17098127" = read_syn_csv("syn17098127"),
    "syn17098128" = read_syn_csv("syn17098128"),
    "syn17098129" = read_syn_csv("syn17098129"),
    "syn17098130" = read_syn_csv("syn17098130"),
    "syn17098131" = read_syn_csv("syn17098131"),
    "syn17098132" = read_syn_csv("syn17098132"),
    "syn17098133" = read_syn_csv("syn17098133"),
    "syn17098134" = read_syn_csv("syn17098134"),
    "syn17098135" = read_syn_csv("syn17098135"),
    "syn17098136" = read_syn_csv("syn17098136"),
    "syn17098137" = read_syn_csv("syn17098137"),
    "syn17098138" = read_syn_csv("syn17098138"),
    "syn17098139" = read_syn_csv("syn17098139"),
    "syn17098140" = read_syn_csv("syn17098140"),
    "syn17098141" = read_syn_csv("syn17098141"),
    "syn17098142" = read_syn_csv("syn17098142"),
    "syn17098143" = read_syn_csv("syn17098143"),
    "syn17098144" = read_syn_csv("syn17098144"),
    "syn17098145" = read_syn_csv("syn17098145"),
    "syn17098146" = read_syn_csv("syn17098146", encoding = "ISO-8859-1"),
    "syn17098147" = read_syn_csv("syn17098147"))
  mjff_merged <- purrr::map(mjff, function(df) {
    df <- mutate(df, user_id = as.character(user_id))
    merged_df <- left_join(df, users, by = "user_id") %>% 
      select(guid, user_id, dplyr::everything())
  })
  return(mjff_merged)
}

store_mjff <- function(mjff_merged) {
  purrr::map2(names(mjff_merged), mjff_merged, function(syn_id, df) {
    file_info <- synGet(syn_id)
    fname <- file_info$properties$name
    temp_dir <- tempdir()
    fpath <- file.path(temp_dir, fname)
    write_csv(df, fpath)
    f <- synapser::File(path = fpath, parent = MJFF_PARENT)
    synStore(f, used = list(syn_id, MJFF_USER_MAPPING))
  })
}

main <- function() {
  synLogin()
  mjff_merged <- merge_mjff_users()
  store_mjff(mjff_merged)
}

main()