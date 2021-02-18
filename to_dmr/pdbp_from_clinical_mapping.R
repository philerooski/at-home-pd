#' This script parses the PDBP data dictionary as well as the clinical data
#' from Rochester and outputs a JSON file that conforms to the schema as
#' defined here: https://github.com/Sage-Bionetworks/at-home-pd/issues/103
#'
#' The resulting JSON file can be used to conform the clinical data to the
#' schema required by the PDBP.

library(tidyverse)
library(synapser)

PDBP_CLINICAL_FIELD_MAP <- "syn23593083"
PDBP_DICTIONARY <- "syn24171997"
CLINICAL_DICTIONARY <- "syn21740194"
COHORT_FIELD_RULES <- list()

main <- function() {

}

main()
