#' This script produces a file with columns:
#' 
#' * externalId (str)
#' * day_one_local_date (date)
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

get_timezone_as_integer <- function(createdOnTimeZone) {
  # If there is no timezone information we make the conservative
  # (for a US user) estimate that the time zone is Pacific
  if (is.na(createdOnTimeZone)) {
    return(-8)
  } else {
    cotz_integer <- as.integer(as.integer(createdOnTimeZone) / 100)
    return(cotz_integer)
  }
}

build_day_one_dates <- function(health_data_summary) {
  day_one_dates <- health_data_summary %>% 
    filter(!is.na(externalId),
           !is.na(dayInStudy),
           originalTable != "sms-messages-sent-from-bridge-v1",
           originalTable != "StudyBurstReminder-v1") %>% 
    distinct(externalId, .keep_all = TRUE) %>% 
    mutate(day_one_local_date = lubridate::as_date(
      createdOnLocalTime - lubridate::days(dayInStudy - 1))) %>%
    select(externalId, day_one_local_date)
  # Some users have not used the app since Bridge rolled out the `dayInStudy` field.
  # We use the old method of computing "Day one" (date of first activity) and
  # hard code the values since there are only two of these users.
  missing_participants <- tibble(
    externalId = c("PDGA-859-GTZ", "PDWP-930-VEE"),
    day_one_local_date = c(lubridate::ymd("2019-06-11", "2019-01-07")))
  day_one_dates <- bind_rows(day_one_dates, missing_participants) %>% 
    arrange(day_one_local_date)
  return(day_one_dates)
}

get_health_data_summary_table <- function() {
  health_data_summary <- read_syn_table(HEALTH_DATA_SUMMARY)
  health_data_summary$createdOnTimeZoneInteger <- unlist(
    purrr::map(health_data_summary$createdOnTimeZone, get_timezone_as_integer))
  health_data_summary <- health_data_summary %>% 
    mutate(createdOnLocalTime = createdOn + lubridate::hours(createdOnTimeZoneInteger))
  return(health_data_summary)
}

main <- function() {
  #synLogin(Sys.getenv("synapseUsername"), Sys.getenv("synapsePassword"))
  synLogin()
  health_data_summary <- get_health_data_summary_table()
  synLogin()
  day_one_dates <- build_day_one_dates(health_data_summary)
  store_to_synapse(day_one_dates)
}

main()
