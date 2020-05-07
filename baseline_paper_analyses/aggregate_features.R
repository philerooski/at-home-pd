library(tidyverse)
library(synapser)
library(glue)

FEATURES <- list(
  "gait_rotation" = "syn21914569",
  "tapping_right" = "syn21960723",
  "tapping_left" = "syn21960721",
  "tremor_right" = "syn21963686",
  "tremor_left" = "syn21963688",
  "tremor_android" = "syn21990578")
OUTPUT_PARENT <- "syn21988713"
HEALTH_DATA_SUMMARY_TABLE <- "syn17015960"

download_feature_files <- function() {
  all_file_entities <- purrr::map(FEATURES, synGet)
  names(all_file_entities) <- names(FEATURES)
  return(all_file_entities)
}

stat_mode <- function(x, return_multiple = TRUE, na.rm = FALSE) {
  if(na.rm){
    x <- na.omit(x)
  }
  ux <- unique(x)
  freq <- tabulate(match(x, ux))
  mode_loc <- if(return_multiple) which(freq==max(freq)) else which.max(freq)
  return(ux[mode_loc])
}

mutate_participant_day <- function(health_data_summary_table) {
  first_activity <- health_data_summary_table %>% 
    group_by(healthCode) %>%
    summarize(first_activity = lubridate::as_date(min(createdOnLocalTime, na.rm = T)))
  health_data_summary_table <- left_join(health_data_summary_table, first_activity)
  health_data_summary_table <- health_data_summary_table %>%
    mutate(createdOnLocalDate = lubridate::as_date(createdOnLocalTime),
           dayInStudy = as.integer(createdOnLocalDate - first_activity) + 1) %>% 
    select(-first_activity, -createdOnLocalDate)
  return(health_data_summary_table)
}

get_timezone_as_integer <- function(createdOnTimeZone) {
  # If there is no timezone information we make the conservative
  # (for a US user) estimate that the time zone is Pacific
  if (is.na(createdOnTimeZone)) {
    return(-8)
  } else {
    cotz_integer <- as.integer(as.integer(createdOnTimeZone) / 100)
    return(cotz_integer)
  }
}

get_health_data_summary_table <- function() {
  health_data_summary_table <- as_tibble(
    synTableQuery(glue("SELECT * FROM { HEALTH_DATA_SUMMARY_TABLE }"))$asDataFrame())
  # Add local time and participant day 
  most_frequent_timezone <- health_data_summary_table %>% 
    filter(originalTable == "StudyBurst-v1") %>% 
    group_by(healthCode) %>%
    summarize(mostFrequentTimeZone = stat_mode(
      createdOnTimeZone, return_multiple = FALSE, na.rm = TRUE))
  health_data_summary_table <- left_join(
    health_data_summary_table, most_frequent_timezone) %>% 
    mutate(mostFrequentTimeZone = replace_na(mostFrequentTimeZone, "-800")) # assume PDT
  health_data_summary_table$createdOnTimeZoneInteger <- unlist(
    purrr::map(health_data_summary_table$mostFrequentTimeZone, get_timezone_as_integer))
  health_data_summary_table <- health_data_summary_table %>%  
    mutate(createdOnLocalTime = createdOn + lubridate::hours(createdOnTimeZoneInteger))
  health_data_summary_table <- mutate_participant_day(health_data_summary_table)
  return(health_data_summary_table)
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

aggregate_gait_rotation_features <- function(path, valid_records) {
  rotation_features <- read_csv(path) %>%
    inner_join(valid_records) %>% 
    filter(test_type == "balance") %>%
    distinct(recordId, window_start, window_end, .keep_all = TRUE) %>%
    arrange(recordId, window_start) %>%
    distinct(recordId, .keep_all = TRUE) %>%
    aggregate_features(agg_feature = rotation_omega,
                       aggregate_by = healthCode,
                       fun = median)
  return(rotation_features)
}

aggregate_tapping_features <- function(path, valid_records,
                                       df = NULL, agg_feature = medianTapInter) {
  if (!is.null(df)) {
    tapping_input <- df
  } else {
    tapping_input <- read_csv(path) 
  }
  tapping_features <- tapping_input %>%
    inner_join(valid_records) %>% 
    mutate(medianTapInter = ifelse( # Android phones recorded in milliseconds
      medianTapInter > 10, medianTapInter / 1000, medianTapInter)) %>%
    group_by(healthCode, hand) %>% 
    summarize({{ agg_feature }} := median({{ agg_feature }}))
  return(tapping_features)
}

#' Tremor features must be aggregated between windows.
#' Measurements along different axes are treated as a continuous measurement
#' Tremor features are only available for iPhone users because
#' Android accelerometer data is not normalized w.r.t. phone orientation
aggregate_tremor_features <- function(path, valid_records,
                                      df = NULL, agg_feature = mean.tm) {
  if (!is.null(df)) {
    tremor_input <- df
  } else {
    tremor_input <- read_csv(path)
  }
  tremor_features <- tremor_input %>%
    inner_join(valid_records) %>% 
    filter(sensor == "accelerometer",
           measurementType == "acceleration",
           !is.na(window)) %>%
    group_by(recordId, healthCode, hand) %>% # no need to average between axes within windows
    summarize({{ agg_feature }} := median({{ agg_feature }})) %>%
    group_by(healthCode, hand) %>% 
    summarize({{ agg_feature }} := median({{ agg_feature }}))
  return(tremor_features)
}

combine_tapping_feature_files <- function(feature_files) {
  tapping_left <- read_csv(feature_files[["tapping_left"]]$path) %>% 
    mutate(hand = "left")
  tapping_right <- read_csv(feature_files[["tapping_right"]]$path) %>% 
    mutate(hand = "right")
  all_tapping <- bind_rows(tapping_left, tapping_right)
  return(all_tapping)
}

combine_tremor_feature_files <- function(feature_files) {
  tremor_left <- read_csv(feature_files[["tremor_left"]]$path) %>% 
    mutate(hand = "left")
  tremor_right <- read_csv(feature_files[["tremor_right"]]$path) %>% 
    mutate(hand = "right")
  tremor_android <- read_csv(feature_files[["tremor_android"]]$path) %>% 
    mutate(hand = case_when(
      col_source == "left_motion.json" ~ "left",
      col_source == "right_motion.json" ~ "right")) %>% 
    select(-col_source)
  all_tremor <- bind_rows(tremor_left, tremor_right) %>% 
    anti_join(tremor_android, by = "recordId") %>% 
    bind_rows(tremor_android)
  return(all_tremor)
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
  valid_records <- get_health_data_summary_table() %>% 
    filter(dayInStudy < 19) %>% 
    select(recordId)
  # Gait rotation time
  rotation_features <- aggregate_gait_rotation_features(
    feature_files[["gait_rotation"]]$path, valid_records)
  store_to_synapse(rotation_features,
                   filename = "gait_rotation.csv",
                   used = list(feature_files[["gait_rotation"]]$properties$id))
  # Tapping median intertap time
  tapping_records <- combine_tapping_feature_files(feature_files)
  all_tapping_features <- aggregate_tapping_features(df = tapping_records,
                                                     valid_records = valid_records)
  store_to_synapse(all_tapping_features,
                   filename = "tapping_medianInterTap.csv",
                   used = list(feature_files[["tapping_right"]]$properties$id,
                               feature_files[["tapping_left"]]$properties$id))
  # Tremor mean of mean.tm across all windows/axes of a record
  tremor_records <- combine_tremor_feature_files(feature_files)
  all_tremor_features <- aggregate_tremor_features(df = tremor_records,
                                                   valid_records = valid_records)
  energy_tremor_features <- aggregate_tremor_features(df = tremor_records,
                                                      valid_records = valid_records,
                                                      agg_feature = energy.tm)
  store_to_synapse(all_tremor_features,
                   filename = "tremor_mean_tm.csv",
                   used = list(feature_files[["tremor_right"]]$properties$id,
                               feature_files[["tremor_left"]]$properties$id))
  store_to_synapse(energy_tremor_features,
                   filename = "tremor_energy_tm.csv",
                   used = list(feature_files[["tremor_right"]]$properties$id,
                               feature_files[["tremor_left"]]$properties$id))
}

main()
