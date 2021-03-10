#################################################
#' Script to generate AT-HOME-PD UPDRS scores
#' to synapse
#' @author: Aryton Tediarjo
##################################################
library(tidyverse)
library(tidyr)
library(data.table)
library(synapser)
source("compute_updrs_score/scoring_utils.R")
synLogin()

#' Synapse reference
CLINICAL_DATA_SYN_ID <- "syn18565500"
EXCEL_LOOKUP_SYN_ID <- "syn25050918"
SYNAPSE_PARENT_ID <- "syn16809549"
OUTPUT_FILENAME <- "computed_ahpd_updrs_scores.tsv"
DMR_FIELD_MAPPING <- "syn25056102"

#' instantiate github
GIT_URL <- file.path(
    "https://github.com/Sage-Bionetworks/at-home-pd/blob/master",
    "compute_updrs_score",
    "compute_updrs_score.R")

#' Final scoring metrics
SCORES <- c(
    "UPDRS1", "UPDRS2", "UPDRS3",
    "UPDRS3R", "UPDRS3", "UPDRSAMB",
    "UPDRSAMR", "UPDRSPRO", "MAX_RTTR",
    "MAX_PKTR", "UPDRSTOT", "TRM_TYPE")

#' function to get AT-HOME-PD clincal data
#' and clean the data by removing testing
#' and filtering based on redcap events
get_ahpd_clinical_data <- function(){
    clinical_records <- fread(synGet(CLINICAL_DATA_SYN_ID)$path) %>%
        tibble::as_tibble(.)
    visit_info <- clinical_records %>%
        dplyr::filter(guid != "TESTING",
                      redcap_event_name == "baseline_arm_1") %>%
        dplyr::select(c("guid":"redcap_repeat_instance"),
                      visstatdttm,
                      viscompltyn,
                      c("mdsupdrs_dttm":"painfloffstatdystn"),
                      matches("neck|postinst_instr"))
    score_info <- clinical_records %>%
        dplyr::filter(guid != "TESTING",
                      redcap_event_name == "baseline_pre_visit_arm_1") %>%
        dplyr::select(c("guid"),
                      c("qstnnreinfoprovdrt":"mdsupdrsfreezingscore"))
    scores <- visit_info %>%
        dplyr::left_join(score_info, by = c("guid")) %>%
        dplyr::filter(is.na(redcap_repeat_instance)) %>%
        dplyr::select(createdOn = mdsupdrs_dttm, everything()) %>%
        tibble::as_tibble(.)
    return(scores)
}

#' Format input for SUPER-PD physician visit
#'
#' @param field_mapping The file referenced above by DMR_FIELD_MAPPING
#' @param physician_visit One of "Physician_ON" or "Physician_OFF"
get_super_physician_scores <- function(field_mapping, physician_visit) {
    clinical_records <- fread(synGet(CLINICAL_DATA_SYN_ID)$path) %>%
        tibble::as_tibble(.)
    relevant_fields <- field_mapping %>%
      filter(form_name == "MDS-UPDRS",
             cohort == "super-pd",
             visit == physician_visit) %>%
      drop_na()
    date_col <- case_when(
        physician_visit == "Physician_OFF" ~ "time_mdsupdrs",
        physician_visit == "Physician_ON" ~ "time_mdsupdrs_off")
    scores <- clinical_records %>%
        dplyr::filter(guid != "TESTING",
                      !is.na(mdsupdrsoffon)) %>%
        dplyr::select(guid, createdOn = {{ date_col }}, relevant_fields$clinical_variable)
    col_map <- readxl::read_excel(synGet(EXCEL_LOOKUP_SYN_ID)$path) %>%
      dplyr::select(
             field_name = `Variable / Field Name`,
             form_name = `Form Name`,
             ctcc_name = `CTCC Name`)
    scores_with_ctcc_names <- scores %>%
      pivot_longer(-c("guid", "createdOn"), names_to="field_name") %>%
      inner_join(col_map, by = "field_name") %>%
      select(guid, createdOn, ctcc_name, value) %>%
      drop_na() %>%
      distinct(guid, ctcc_name, .keep_all=T) %>%
      mutate(ctcc_name = unlist(purrr::map(ctcc_name, ~ paste0("C_", .)))) %>%
      pivot_wider(id_cols=c("guid", "createdOn"),
                  names_from="ctcc_name",
                  values_from="value")
    updrs_scores <- scores_with_ctcc_names %>%
      compute_updrs_total_scores(join_cols=c("guid", "createdOn")) %>%
      select(guid, createdOn, UPDRS1, UPDRS2, UPDRS3, UPDRS3R,
             UPDRS4, UPDRSAMB, UPDRSAMR, UPDRSPRO)
    return(updrs_scores)
}

store_to_synapse <- function(df, fname, parent) {
    readr::write_csv(df, fname)
    f <- synapser::File(fname, parent)
    synapser::synStore(f, activity = Activity(
        "compute updrs scores",
        used = c(CLINICAL_DATA_SYN_ID,
                 EXCEL_LOOKUP_SYN_ID),
        executed = GIT_URL))
    unlink(fname)
}

main <- function(){
    field_mapping <- readr::read_csv(synapser::synGet(DMR_FIELD_MAPPING)$path)
    #' compute updrs score and write to .tsv
    super_physician_on <- get_super_physician_scores(field_mapping, "Physician_ON")
    store_to_synapse(super_physician_on, "mdsupdrs_physician_on_med_scores.csv", "syn18691015")
    super_physician_off <- get_super_physician_scores(field_mapping, "Physician_OFF")
    store_to_synapse(super_physician_off, "mdsupdrs_physician_off_med_scores.csv", "syn18691015")
    ahpd_updrs_scores <- get_ahpd_clinical_data() %>%
        map_column_names("ahpd") %>%
        run_updrs_scoring(
            join_cols = c("guid", "createdOn", "C_ONLDOPA")) %>%
        write.table(OUTPUT_FILENAME, sep = "\t", row.names=F, quote=F)
    #' store result to synapse
    f <- File(OUTPUT_FILENAME, SYNAPSE_PARENT_ID)
    synStore(f, activity = Activity(
        "compute updrs scores",
        used = c(CLINICAL_DATA_SYN_ID,
                 EXCEL_LOOKUP_SYN_ID),
        executed = GIT_URL))
    unlink(OUTPUT_FILENAME)
}
main()

