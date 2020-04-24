library(tidyverse)
library(synapser)

FEATURES <- list(
  "gait_rotation" = "syn21914569",
  "tapping_right" = "syn21960723",
  "tapping_left" = "syn21960721",
  "tremor_right" = "syn21963686",
  "tremor_left" = "syn21963688")

download_feature_files <- function() {
  all_file_entities <- purrr::map(FEATURES, synGet)
  names(all_file_entities) <- names(FEATURES)
  return(all_file_entities)
}

#' Compute a summary statistic within groups
#' 
#' @param features A dataframe
#' @param agg_feature The metric or measure to summarize
#' @param aggregate_by The column to group upon before computing 
#' the intra-group summary statistic
#' @parm fun the intra-group summary function
#' @return A dataframe of summary statistics for each group in `aggregate_by`
aggregate_features <- function(features, agg_feature,
                               aggregate_by, fun = median) {
  features %>% 
    group_by({{ aggregate_by }}) %>% 
    summarize({{ agg_feature }} := fun({{ agg_feature }}))
}

aggregate_gait_rotation_features <- function(path) {
  rotation_features <- read_csv(path) %>% 
    filter(test_type == "balance") %>% 
    distinct(recordId, window_start, window_end, .keep_all = TRUE) %>% 
    arrange(recordId, window_start) %>% 
    distinct(recordId, .keep_all = TRUE) %>%
    aggregate_features(agg_feature = rotation_omega,
                       aggregate_by = healthCode,
                       fun = median)
  return(rotation_features)
}

aggregate_tapping_features <- function(path) {
  
}

main <- function() {
  synLogin()
  feature_files <- download_feature_files()
  # Gait rotation time
  rotation_features <- aggregate_gait_rotation_features(
    feature_files[[FEATURES[["gait_rotation"]]]]$path) 
  # Tapping median intertap time
  tapping_left_features <- aggregate_tapping_features(
    features_files[[FEATURES[["tapping_left"]]]]$path)
}

#main()