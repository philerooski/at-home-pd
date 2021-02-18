#' Given AT-HOME PD clinical data from REDCap, translate each record into a
#' format which conforms to the appropriate PDBP DMR schema.

library(dplyr)

#' Parse mandatory DMR schema fields from a clinical record
get_universal_fields <- function(record, visit_date_col, dob_mapping,
                                 cohort, redcap_event_name) {
  universal_fields <- tibble::tibble(
      GUID = record$guid,
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

#' Parse concomitant medication record for DMR
#'
#' Participants can have multiple concomitant medication records for a single
#' visit, hence this type of record needs to be handled specially.
#' This function extracts fields MedctnPriorConcomRteTyp, MedctnPriorConcomName,
#' MedctnPriorConcomDoseMsrTxt, MedctnPriorConcomDoseUoM, MedctnPriorConcomFreqTxt,
#' and MedctnPriorConcomPD. Other fields specific to this form are set to NA.
parse_concomitant_medication_record <- function(record, mapping) {
  med_map <- mapping[["concomitant_medication_log"]]
  is_pd_med <- record$pd_med_yn == "Yes"
  route <- value_map(med_map, "route", record$route)
  route_oth <- value_map(med_map, "route_oth", record$route_oth)
  pd_meds <- value_map(med_map, "pd_meds", record$pd_meds)
  pd_med_oth <- value_map(med_map, "pd_med_oth", record$pd_med_oth)
  freq <- value_map(med_map, "freq", record$freq)
  freq_oth <- value_map(med_map, "freq_oth", record$freq_oth)
  dose_numeric <- ifelse(is.na(as.numeric(record$dose)), NA, record$dose)
  dmr_record <- tibble(
      MedctnPriorConcomRteTyp = case_when(
        !is.null(route) ~ route,
        !is.null(route_oth) ~ route_oth,
        TRUE ~ record$route),
      MedctnPriorConcomPD = case_when(
        is_pd_med && !is.null(pd_meds) ~ pd_meds,
        is_pd_med && pd_med_oth != "" ~ pd_med_oth,
        TRUE ~ record$pd_med_oth),
      MedctnPriorConcomName = case_when(
        is_pd_med && pd_med_oth == "" ~ record$pd_med_other,
        !is_pd_med ~ record$non_pd_med),
      MedctnPriorConcomMinsLstDose = NA,
      MedctnPriorConcomHrsLstDose = NA,
      MedctnPriorConcomIndTxt = record$indication
      MedctnPriorConcomFreqTxt = case_when(
        !is.null(freq) ~ freq,
        !is.null(freq_oth) ~ freq_oth,
        TRUE ~ record$freq_oth),
      MedctnPriorConcomDoseMsrTxt = case_when
      )
}

value_map <- function(mapping, field, key) {
  value <- mapping[[field]][[key]]
  return(value)
}

main <- function() {

}
