#' Given AT-HOME PD clinical data from REDCap, translate each record into a
#' format which conforms to the appropriate PDBP DMR schema.

library(dplyr)

#' Parse mandatory DMR schema fields from a clinical record
get_universal_fields <- function(record, visit_date_col, dob_mapping,
                                 cohort, redcap_event_name) {
  universal_fields <- tibble::tibble(
      guid = record$guid,
      VisitDate = record[[visit_date_col]],
      SiteName = "AT-HOME-PD_University of Rochester",
      AgeVal = get_age_in_months(
          current_date = record[[visit_date_col]],
          dob = dob_mapping[[record$guid]]),
      VisitTypPDBP = get_visit_type(cohort, redcap_event_name))
  universal_fields[["AgeYrs"]] <- universal_fields$AgeVal %/% 12
  universal_fields[["AgeRemaindrMonths"]] <- universal_fields$AgeVal %% 12
  return(universal_fields)
}

#' Compute current age in months
#'
#' The following participants do not provide the necessary fields to compute
#' their current age:
#' 1 NIHGE434YJLLA
#' 2 NIHMR963TPLWF
#' 3 PDGA-859-GTZ
#' 4 PDRJ-711-MKF
get_age_in_months <- function(current_date, dob) {
  if (anyNA(c(current_date, dob))) {
      return(NA)
  }
  age_in_months <- lubridate::months(current_date - dob)
  return(age_in_months)
}

#' Get VisitTypPDBP field from cohort and event name
get_visit_type <- function(cohort, redcap_event_name) {
  visit_type <- case_when(
    cohort == "at-home-pd" && str_detect(redcap_event_name, "Baseline") ~ "Baseline",
    cohort == "at-home-pd" && str_detect(redcap_event_name, "Screening") ~ "Baseline",
    cohort == "at-home-pd" && str_detect(redcap_event_name, "Month 12") ~ "Month 12",
    cohort == "at-home-pd" && str_detect(redcap_event_name, "Month 24") ~ "Month 24",
    cohort == "super-pd" ~ "Baseline",
    TRUE ~ "Baseline") # Logs and Premature Withdrawal
  return(visit_type)
}

#' Build a mapping of GUID to date of birth
build_dob_mapping <- function(clinical) {
  ahpd_dob <- clinical %>%
      filter(!is.na(dob)) %>%
      select(guid, dob)
  super_dob <- clinical %>%
      filter(!is.na(demo_age), !is.na(visitdate)) %>%
      mutate(dob = (visitdate - lubridate::dyears(demo_age))) %>%
      select(guid, dob)
  all_dob <- bind_rows(ahpd_dob, super_dob)
  return(all_dob)
}

main <- function() {

}
