library(tidyverse)
library(synapser)

FEATURES <- list(
  "gait_rotation" = "syn21914569",
  "tapping_right" = "syn21960723",
  "tapping_left" = "syn21960721",
  "tremor_right" = "syn21963686",
  "tremor_left" = "syn21963688")
OUTPUT_PARENT <- "syn21988713"

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

aggregate_tapping_features <- function(path, df = NULL) {
  if (!is.null(df)) {
    tapping_input <- df
  } else {
    tapping_input <- read_csv(path)
  }
  tapping_features <- tapping_input %>%
    distinct(recordId, .keep_all = TRUE) %>%
    mutate(medianTapInter = ifelse( # Android phones recorded in milliseconds
      medianTapInter > 10, medianTapInter / 1000, medianTapInter)) %>%
    aggregate_features(agg_feature = medianTapInter,
                       aggregate_by = healthCode,
                       fun = median)
  return(tapping_features)
}

#' Tremor features must be aggregated between windows.
#' Measurements along different axes are treated as a continuous measurement
#' Tremor features are only available for iPhone users because
#' Android accelerometer data is not normalized w.r.t. phone orientation
aggregate_tremor_features <- function(path, df = NULL, agg_feature = mean.tm) {
  if (!is.null(df)) {
    tremor_input <- df
  } else {
    tremor_input <- read_csv(path)
  }
  tremor_features <- tremor_input %>%
    filter(sensor == "accelerometer",
           measurementType == "acceleration",
           !is.na(window)) %>%
    group_by(recordId, healthCode) %>% # no need to average between axes within windows
    summarize({{ agg_feature }} := mean({{ agg_feature }})) %>%
    aggregate_features(agg_feature = {{ agg_feature }},
                       aggregate_by = healthCode,
                       fun = median)
  return(tremor_features)
}

store_to_synapse <- function(feature_set, filename, used, parent = OUTPUT_PARENT) {
  write_csv(feature_set, filename)
  f <- synapser::File(filename, parent = parent)
  synStore(f, used = used)
  unlink(filename)
}

main <- function() {
  synLogin()
  feature_files <- download_feature_files()
  # Gait rotation time
  rotation_features <- aggregate_gait_rotation_features(
    feature_files[["gait_rotation"]]$path)
  store_to_synapse(rotation_features,
                   filename = "gait_rotation.csv",
                   used = list(feature_files[["gait_rotation"]]$properties$id))
  # Tapping median intertap time
  tapping_left_features <- aggregate_tapping_features(
    feature_files[["tapping_left"]]$path)
  tapping_right_features <- aggregate_tapping_features(
    feature_files[["tapping_left"]]$path)
  tapping_records <- purrr::map_dfr(
    feature_files[c("tapping_left", "tapping_right")],
    ~ read_csv(.$path))
  all_tapping_features <- aggregate_tapping_features(df = tapping_records)
  store_to_synapse(tapping_left_features,
                   filename = "tapping_left_medianInterTap.csv",
                   used = list(feature_files[["tapping_left"]]$properties$id))
  store_to_synapse(tapping_right_features,
                   filename = "tapping_right_medianInterTap.csv",
                   used = list(feature_files[["tapping_right"]]$properties$id))
  store_to_synapse(all_tapping_features,
                   filename = "tapping_both_medianInterTap.csv",
                   used = list(feature_files[["tapping_right"]]$properties$id,
                               feature_files[["tapping_left"]]$properties$id))
  # Tremor mean of mean.tm across all windows/axes of a record
  tremor_left_features <- aggregate_tremor_features(
    feature_files[["tremor_left"]]$path)
  tremor_right_features <- aggregate_tremor_features(
    feature_files[["tremor_left"]]$path)
  tremor_records <- purrr::map_dfr(
    feature_files[c("tremor_left", "tremor_right")],
    ~ read_csv(.$path))
  all_tremor_features <- aggregate_tremor_features(df = tremor_records)
  store_to_synapse(tremor_left_features,
                   filename = "tremor_left_features.csv",
                   used = list(feature_files[["tremor_left"]]$properties$id))
  store_to_synapse(tremor_right_features,
                   filename = "tremor_right_features.csv",
                   used = list(feature_files[["tremor_right"]]$properties$id))
  store_to_synapse(all_tremor_features,
                   filename = "tremor_both_features.csv",
                   used = list(feature_files[["tremor_right"]]$properties$id,
                               feature_files[["tremor_left"]]$properties$id))
}

main()
