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
SYNAPSE_PARENT_ID <- "syn16809549"
OUTPUT_FILENAME <- "computed_ahpd_updrs_scores.tsv"

#' instantiate github
GIT_URL <- file.path(
    "https://github.com/arytontediarjo/at-home-pd/blob/master",
    "compute_updrs_score", 
    "compute_updrs_score.R")
    
#' Global Variables
CTCC_UPDRS_LOOKUP <- file.path(
    "compute_updrs_score", 
    "ATHOMEPD_CTCC_MDS-UPDRS_lookup_20200229_edited.xlsx")
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

main <- function(){
    #' compute updrs score and write to .tsv
    ahpd_updrs_scores <- get_ahpd_clinical_data() %>%
        map_column_names("ahpd") %>%
        run_updrs_scoring(
            join_cols = c("guid", "createdOn", "C_ONLDOPA")) %>%
        write.table(OUTPUT_FILENAME , sep = "\t", row.names=F, quote=F)
    #' store result to synapse
    f <- File(OUTPUT_FILENAME, SYNAPSE_PARENT_ID)
    synStore(f, activity = Activity(
        "retrieve raw walk features",
        used = c(CLINICAL_DATA_SYN_ID),
        executed = GIT_URL))
    unlink(OUTPUT_FILENAME)
}
main()

