#' This script produces a file with columns:
#' 
#' * externalId (str)
#' * day_one (date)
#' 
#' And stores the output to OUTPUT (global var below)
library(synapser)
library(tidyverse)

HEALTH_DATA_SUMMARY <- "syn17015960"
OUTPUT <- "syn16809551"

read_syn_table <- function(syn_id) {
  q <- synTableQuery(paste("select * from", syn_id))
  table <- q$asDataFrame() %>% 
    as_tibble() 
  return(table)
}

store_to_synapse <- function(day_one_dates) {
  fpath <- "day_one_dates.csv"
  write_csv(day_one_dates, fpath)
  f <- synapser::File(fpath, parent = OUTPUT)  
  synStore(f)
  unlink(fpath)
}

build_day_one_dates <- function(health_data_summary) {
  day_one_dates <- health_data_summary %>% 
    filter(!is.na(externalId), !is.na(dayInStudy)) %>% 
    distinct(externalId, .keep_all = TRUE) %>% 
    mutate(day_one = lubridate::as_date(
      createdOn - lubridate::days(dayInStudy - 1))) %>%
    select(externalId, day_one)
  # Some users have not used the app since Bridge rolled out the `dayInStudy` field.
  # We use the old method of computing "Day one" (date of first activity) and
  # hard code the values since there are only two of these users.
  missing_participants <- tibble(
    externalId = c("PDGA-859-GTZ", "PDWP-930-VEE"),
    day_one = c(lubridate::ymd("2019-06-11", "2019-01-07"))
  )
  day_one_dates <- bind_rows(day_one_dates, missing_participants) %>% 
    arrange(day_one)
  return(day_one_dates)
}

main <- function() {
  synLogin(Sys.getenv("synapseUsername"), Sys.getenv("synapsePassword"))
  health_data_summary <- read_syn_table(HEALTH_DATA_SUMMARY)
  day_one_dates <- build_day_one_dates(health_data_summary)
  store_to_synapse(day_one_dates)
}

main()
