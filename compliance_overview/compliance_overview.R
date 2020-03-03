#' This script produces a table with columns:
#' 
#' * study_burst (str)
#' * complete (int)
#' * partially_complete (int)
#' * to_complete (int)
#' * percent_complete (bool)
#' 
#' And stores the output to TABLE_OUTPUT (global var below)
library(synapser)
library(tidyverse)

STUDY_BURST_SUMMARY <- "syn20930854"
STUDY_BURST_SCHEDULE <- "syn20930854"
TABLE_OUTPUT <- "syn21500168"

read_syn_table <- function(syn_id) {
  q <- synTableQuery(paste("select * from", syn_id))
  table <- q$asDataFrame() %>% 
    as_tibble() 
  return(table)
}

store_to_synapse <- function(compliance_overview) {
  q <- synTableQuery(paste("select * from", TABLE_OUTPUT))
  synDelete(q) # Remove preexisting rows
  t <- synTable(TABLE_OUTPUT, compliance_overview)  
  synStore(t)
}

build_compliance_overview <- function(study_burst_summary, study_burst_schedule) {
  num_no_activity <- study_burst_schedule %>% 
    filter(lubridate::as_date(study_burst_start_date) <= lubridate::today(),
           days_completed == 0) %>% 
    count(study_burst, name = "no_activity")
  compliance_overview <- study_burst_summary %>% 
    group_by(study_burst) %>% 
    summarize(complete = sum(study_burst_successful, na.rm = T),
              partially_complete = sum(!study_burst_successful, na.rm = T),
              to_complete = sum(is.na(study_burst_successful)),
              percent_compliant =  round(complete / (complete + partially_complete), 2)) %>% 
    left_join(num_no_activity) %>% 
    mutate(no_activity = replace_na(no_activity, 0),
           partially_complete = partially_complete - no_activity) %>% 
    select(study_burst, complete, partially_complete, no_activity,
           to_complete, percent_compliant) %>% 
    arrange(study_burst)
  return(compliance_overview)
}

main <- function() {
  synLogin(Sys.getenv("synapseUsername"), Sys.getenv("synapsePassword"))
  study_burst_summary <- read_syn_table(STUDY_BURST_SUMMARY)
  study_burst_schedule <- read_syn_table(STUDY_BURST_SCHEDULE)
  compliance_overview <- build_compliance_overview(study_burst_summary,
                                                   study_burst_schedule)
  store_to_synapse(compliance_overview)
}

main()
