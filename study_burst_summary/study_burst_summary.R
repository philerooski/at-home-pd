library(synapser)
library(tidyverse)

MPOWER_SUMMARY <- "syn18693245"
USER_OFFSET <- "syn18637903"
TABLE_OUTPUT <- "syn20930854"

read_syn_table <- function(syn_id) {
  q <- synTableQuery(paste("select * from", syn_id))
  table <- q$asDataFrame() %>% 
    as_tibble() 
  return(table)
}

fetch_mpower <- function() {
  user_offset <- read_syn_table(USER_OFFSET) %>% 
    select(-ROW_ID, -ROW_VERSION)
  mpower <- read_syn_table(MPOWER_SUMMARY) %>% 
    filter(source == "MPOWER") %>% 
    left_join(user_offset, by = c("guid", "source")) %>% 
    mutate(createdOn = lubridate::with_tz(createdOn, "America/Los_Angeles"),
           createdOn = createdOn - lubridate::days(day_offset)) %>% 
    select(-day_offset)
  return(mpower)
}

build_study_burst_summary <- function(mpower) {
  first_activity <- mpower %>% 
    group_by(activity_guid = guid) %>%
    summarize(first_activity = lubridate::as_date(min(createdOn)),
              currentDayInStudy = as.integer(
                lubridate::today(tz="America/Los_Angeles") - first_activity),
              currentlyInStudyBurst = currentDayInStudy %% 90 < 20)
  study_burst_dates <- purrr::map2_dfr(
    first_activity$activity_guid, first_activity$first_activity, function(guid, first_activity) {
    dates <- purrr::map_dfr(0:7, function(i) {
      tibble(dates_guid = guid,
             study_burst_number = i,
             study_burst_start_date = first_activity + i * lubridate::days(90),
             study_burst_end_date = study_burst_start_date + lubridate::days(19))
    })                                      
    return(dates)
  })
  days_completed <- purrr::pmap_dfr(first_activity,
    function(activity_guid, first_activity, currentDayInStudy, currentlyInStudyBurst) {
      relevant_study_burst_dates <- study_burst_dates %>%
        filter(dates_guid == activity_guid)
      days_completed <- purrr::pmap_dfr(relevant_study_burst_dates,
        function(dates_guid, study_burst_number, study_burst_start_date, study_burst_end_date) {
          relevant_mpower <- mpower %>%
            filter(guid == dates_guid, createdOn >= study_burst_start_date, createdOn <= study_burst_end_date)
          days_completed_this_burst <- n_distinct(relevant_mpower$dayInStudy)
          # If the participant has not yet finished this study burst, store NA for days completed
          if (days_completed_this_burst == 0 && study_burst_end_date >= lubridate::today()) {
            days_completed_this_burst = NA
          }
          tibble(days_guid = dates_guid,
                 study_burst_number = study_burst_number,
                 days_completed = days_completed_this_burst,
                 study_burst_successful = days_completed >= 10)
      })
      return(days_completed)
  })
  study_burst_summary <- mpower %>% 
    distinct(guid) %>% 
    left_join(study_burst_dates, by = c("guid" = "dates_guid")) %>% 
    left_join(days_completed, by = c("guid" = "days_guid", "study_burst_number")) %>% 
    rename(study_burst = study_burst_number) %>% 
    mutate(study_burst = as.character(study_burst),
           study_burst_start_date = as.character(study_burst_start_date),
           study_burst_end_date = as.character(study_burst_end_date),
           guid_prefix = str_extract(guid, "^.{3}")) %>% 
    arrange(guid, study_burst) %>% 
    select(guid, guid_prefix, study_burst, study_burst_start_date,
           study_burst_end_date, days_completed, study_burst_successful)
  study_burst_summary$study_burst <- dplyr::recode(
    study_burst_summary$study_burst, "0" = "Y1,Q1", "1" = "Y1,Q2", "2" = "Y1,Q3",
    "3" = "Y1,Q4", "4" = "Y2,Q1", "5" = "Y2,Q2", "6" = "Y2,Q3",
    "7" = "Y2,Q4")
  return(study_burst_summary)
}

store_to_synapse <- function(study_burst_summary) {
  q <- synTableQuery(paste("select * from", TABLE_OUTPUT))
  synDelete(q) # Remove preexisting rows
  t <- synTable(TABLE_OUTPUT, study_burst_summary)  
  synStore(t)
}

main <- function() {
  synLogin(Sys.getenv("synapseUsername"), Sys.getenv("synapsePassword"))
  mpower <- fetch_mpower()
  study_burst_summary <- build_study_burst_summary(mpower)
  store_to_synapse(study_burst_summary)
}

main()
