#################################################
#' Utility function to compute UPDRS scores
#' It will measure metrics based on Eric SAS code
#' @author: Aryton Tediarjo
##################################################

#' function to retrieve NA
#' thresholds for each UPDRS metrics
get_na_threshold_conditions <- function(){
    thresholds <- list()
    thresholds$UPDRS1 <- 1
    thresholds$UPDRS2 <- 2
    thresholds$UPDRS3 <- 7
    thresholds$UPDRS3R <- 6
    thresholds$UPDRS4 <- 0
    thresholds$UPDRSAMB <- 0
    thresholds$UPDRSAMR <- 0
    thresholds$UPDRSPRO <- 0
    return(thresholds)
}


#' function to retrieve reference based on SAS code
get_updrs_score_reference <- function(){
    UPDRS_LIST <- list()
    UPDRS_LIST$UPDRS1 <- c("C_NP1COG", "C_NP1HALL",
                           "C_NP1DPRS","C_NP1ANXS",
                           "C_NP1APAT", "C_NP1DDS",
                           "C_NP1SLPN", "C_NP1SLPD",
                           "C_NP1PAIN", "C_NP1URIN",
                           "C_NP1CNST", "C_NP1LTHD",
                           "C_NP1FATG")
    UPDRS_LIST$UPDRS2 <- c("C_NP2SPCH", "C_NP2SALV",
                           "C_NP2SWAL", "C_NP2EAT",
                           "C_NP2DRES", "C_NP2HYGN",
                           "C_NP2HWRT", "C_NP2HOBB",
                           "C_NP2TURN", "C_NP2TRMR",
                           "C_NP2RISE", "C_NP2WALK",
                           "C_NP2FREZ")
    UPDRS_LIST$UPDRS3 <- c(
        "C_NP3SPCH",  "C_NP3FACXP",
        "C_NP3RIGN", "C_NP3RIGRU",
        "C_NP3RIGLU", "C_NP3RIGRL",
        "C_NP3RIGLL", "C_NP3FTAPR",
        "C_NP3FTAPL", "C_NP3HMOVR",
        "C_NP3HMOVL", "C_NP3PRSPR",
        "C_NP3PRSPL", "C_NP3TTAPR",
        "C_NP3TTAPL", "C_NP3LGAGR",
        "C_NP3LGAGL", "C_NP3RISNG",
        "C_NP3GAIT",  "C_NP3FRZGT",
        "C_NP3POSTR", "C_NP3BRADY",
        "C_NP3PTRMR", "C_NP3PTRML",
        "C_NP3KTRMR", "C_NP3KTRML",
        "C_NP3RTARU", "C_NP3RTALU",
        "C_NP3RTARL", "C_NP3RTALL",
        "C_NP3RTALJ", "C_NP3RTCON", "C_NP3PSTBL")
    UPDRS_LIST$UPDRS3R <- c("C_NP3SPCH", "C_NP3FACXP", "C_NP3FTAPR",
                            "C_NP3FTAPL", "C_NP3HMOVR", "C_NP3HMOVL",
                            "C_NP3PRSPR", "C_NP3PRSPL", "C_NP3TTAPR",
                            "C_NP3TTAPL", "C_NP3LGAGR", "C_NP3LGAGL",
                            "C_NP3RISNG", "C_NP3GAIT", "C_NP3FRZGT",
                            "C_NP3POSTR", "C_NP3BRADY", "C_NP3PTRMR",
                            "C_NP3PTRML", "C_NP3KTRMR", "C_NP3KTRML",
                            "C_NP3RTARU", "C_NP3RTALU", "C_NP3RTARL",
                            "C_NP3RTALL", "C_NP3RTALJ", "C_NP3RTCON")
    UPDRS_LIST$UPDRS4 <- c("C_NP4DYSKI", "C_NP4DYSTN", "C_NP4FLCTI",
                           "C_NP4FLCTX", "C_NP4OFF", "C_NP4WDYSK")
    UPDRS_LIST$UPDRSAMB <- c("C_NP2WALK", "C_NP2FREZ", "C_NP3GAIT",
                             "C_NP3FRZGT", "C_NP3PSTBL")
    UPDRS_LIST$UPDRSAMR <- c("C_NP2WALK", "C_NP2FREZ", "C_NP3GAIT", "C_NP3FRZGT")
    UPDRS_LIST$UPDRSPRO <-  c("C_NP1SLPN", "C_NP1SLPD", "C_NP1PAIN",
                              "C_NP1URIN", "C_NP1CNST", "C_NP1LTHD",
                              "C_NP1FATG", "C_NP2SPCH", "C_NP2SALV",
                              "C_NP2SWAL", "C_NP2EAT", "C_NP2DRES",
                              "C_NP2HYGN", "C_NP2HWRT", "C_NP2HOBB",
                              "C_NP2TURN", "C_NP2TRMR", "C_NP2RISE",
                              "C_NP2WALK", "C_NP2FREZ")
    UPDRS_LIST$MAX_PKTR <- c("C_NP3PTRMR", "C_NP3PTRML",
                             "C_NP3KTRMR", "C_NP3KTRML")
    UPDRS_LIST$MAX_RTTR <- c("C_NP3RTARU", "C_NP3RTALU", "C_NP3RTARL",
                             "C_NP3RTALL", "C_NP3RTALJ")
    return(UPDRS_LIST)
}

#' function to get mapping on what data are being used for UPDRS computation
#' @param updrs_metric: which updrs metrics you are looking for
#' (UPDRS1, UPDRS2, UPDRS3, UPDRS3R, UPDRSTOT, UPDRSAMBL, UPDRSAMR, MAX_PKTR, MAX_RTTR)
get_updrs_mapping <- function(updrs_metric = NULL){
    mapping <- readxl::read_excel(synGet(EXCEL_LOOKUP_SYN_ID)$path) %>%
        dplyr::filter(`Form Name` %in%
                          c("mdsupdrs",
                            "prebaseline_survey",
                            !is.na(`CTCC Name`)) |
                          str_detect(`Variable / Field Name`, "neck|postinst_instr")) %>%
        dplyr::select(ahpd = `Variable / Field Name`,
                      ctcc_code = `CTCC Name`,
                      mpower = `Section Header`) %>%
        dplyr::rowwise() %>%
        dplyr::filter(!is.na(ctcc_code)) %>%
        dplyr::mutate(
            mpower = str_extract(mpower, "[1-9].[0-9]{1,3}"),
            mpower = ifelse(!is.na(mpower),
                            glue::glue("MDS-UPDRS", mpower),
                            mpower),
            ctcc_code = glue::glue("C_",ctcc_code)) %>%
        dplyr::select(ctcc_code, everything())
    if(!is.null(updrs_metric)){
        mapping %>%
            dplyr::filter(ctcc_code %in% get_updrs_score_reference()[[updrs_metric]])
    }else{
        mapping
    }
}


#' Function to map column names to match required dataframe for
#' computing updrs scores
#' @param data: the data being parsed in (clinical or mpower table)
#' @param data_opts: parse in field options ("mpower_field", "ahpd_field")
#' @return mapped column name
map_column_names <- function(data, data_opts){
    if(!data_opts %in% c("mpower", "ahpd")){
        stop("Please parse in parameter 'mpower' or 'ahpd'")
    }
    updrs_cols_lookup <- get_updrs_mapping() %>%
        dplyr::select(all_of(data_opts), ctcc_code) %>%
        tidyr::drop_na()
    data <- tryCatch({
        data %>%
            data.table::setnames(., old = updrs_cols_lookup[[data_opts]],
                                 new = updrs_cols_lookup$ctcc_code, skip_absent = TRUE)
    }, error = function(e){
        message("caught an error")
        message(e)
    })
    return(data)
}


#' This function is used to gather UPDRS curation info
#' such as what are the available columns,
#' setting the non_na threshold based from sas code
#' and the data itself
#' @param data: dataframe, preferabbly from Synapse Table format
#' @param updrs_metrics: the metric used for assessment
#' @return named list of data, detected_columns, ncols, na_threshold, and updrs metrics
map_updrs_curate_info <- function(data,
                                  updrs_metrics,
                                  keep_cols = c("recordId", "createdOn")){
    updrs_curate_info <- list()
    updrs_cols <- get_updrs_score_reference()[[updrs_metrics]]
    updrs_curate_info$data <- data %>%
        dplyr::select(any_of(keep_cols), any_of(updrs_cols))
    updrs_curate_info$detected_cols <- updrs_curate_info$data %>%
        dplyr::select(-any_of(keep_cols)) %>%
        names(.)
    updrs_curate_info$cols_not_available <- setdiff(updrs_cols, updrs_curate_info$detected_cols)
    updrs_curate_info$ncols <- updrs_curate_info$detected_cols %>% length(.)
    updrs_curate_info$na_thresh <- updrs_curate_info$ncols - get_na_threshold_conditions()[[updrs_metrics]]
    updrs_curate_info$updrs_metrics <- updrs_metrics
    return(updrs_curate_info)
}

#' Function to compute updrs total scores
#' @param data: dataset to parse in (preferrably has mapped columns)
#' @param join_cols: columns to keep and used for joining each metrics
#' @return returns a joined total score of updrs
compute_updrs_total_scores <- function(data, join_cols){
    updrs_metrics_list <- c("UPDRS1", "UPDRS2",
                            "UPDRS3", "UPDRS3R",
                            "UPDRS4", "UPDRSAMB",
                            "UPDRSAMR", "UPDRSPRO")
    purrr::map(updrs_metrics_list, function(metric){
        message(glue::glue("calculating UPDRS metric: ", metric))
        result <- tryCatch({
            updrs_info <- data %>%
              map_updrs_curate_info(metric, keep_cols = join_cols)
            updrs_info$data %>%
                dplyr::mutate(non_na_count = rowSums(!is.na(select(., any_of(
                    get_updrs_score_reference()[[metric]]))))) %>%
                dplyr::filter(non_na_count >= updrs_info$na_thresh) %>%
                dplyr::rowwise() %>%
                dplyr::mutate(
                    !!updrs_info$updrs_metrics := round(mean(c_across(all_of(
                        updrs_info$detected_cols)), na.rm = T) *
                            updrs_info$ncols, 0)) %>%
                dplyr::ungroup() %>%
                dplyr::select(-non_na_count)
        }, error = function(e){
            data %>%
                dplyr::mutate(!!updrs_info$updrs_metrics := NA) %>%
                dplyr::select(all_of(join_cols, !!updrs_info$updrs_metrics))
        })
    }) %>% purrr::reduce(., dplyr::full_join)
}

#' Function to compute updrs maximum scores
#' @param data: dataset to parse in (preferrably has mapped columns)
#' @param join_cols: columns to keep and used for joining each metrics
#' @return returns a joined max scores of updrs
compute_updrs_max_scores <- function(data, join_cols){
    updrs_metrics_list <- c("MAX_RTTR", "MAX_PKTR")
    purrr::map(updrs_metrics_list, function(metric){
        message(glue::glue("calculating UPDRS metric: ", metric))
        updrs_info <- data %>% map_updrs_curate_info(metric, keep = join_cols)
        result <- tryCatch({
            updrs_info$data %>%
                dplyr::rowwise() %>%
                dplyr::mutate(
                    !!updrs_info$updrs_metrics := max(c_across(updrs_info$detected_cols), na.rm = TRUE)) %>%
                dplyr::mutate_if(is.numeric, ~ifelse(abs(.) == Inf,NA,.))
        }, error = function(e){
            updrs_info$data %>%
                dplyr::mutate(!!updrs_info$updrs_metrics := NA)
        })
    }) %>% purrr::reduce(., dplyr::full_join)
}

#' Function to compute updrs combined scores
#' @param data: dataset to parse in (preferrably has mapped columns)
#' @param join_cols: columns to keep and used for joining each metrics
#' @return returns a joined max scores of updrs
compute_updrs_combined_score <- function(data){
    data %>%
        dplyr::rowwise() %>%
        dplyr::mutate(UPDRSTOT = sum(c_across(c("UPDRS1", "UPDRS2", "UPDRS3")))) %>%
        dplyr::mutate(
            TRM_TYPE = case_when(
                (MAX_PKTR == 0 & MAX_RTTR == 0) ~ 0,
                (MAX_PKTR >= MAX_RTTR) ~ 1,
                (MAX_PKTR < MAX_RTTR) ~ 2,
                TRUE ~ NA_real_))
}

#' entry function for running updrs scoring
#' @param data: dataset where you want to run the scoring
#' @param join_cols: columns to keep or that will be used for joining
run_updrs_scoring <- function(data, join_cols){
    list(total = data %>%
             compute_updrs_total_scores(join_cols),
         max = data %>%
             compute_updrs_max_scores(join_cols)) %>%
        purrr::reduce(dplyr::full_join) %>%
        compute_updrs_combined_score() %>%
        dplyr::relocate(all_of(SCORES), .after = last_col())
}


