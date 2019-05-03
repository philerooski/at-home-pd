library(synapser)
library(tidyverse)

MJFF_USERS <- "syn17098120"
ROCHESTER_USERS <- "syn17051543"
BRIDGE_USERS <- "syn16786935"
ROCHESTER_PARENT <- "syn18637131"
MJFF_PARENT <- "syn18637133"
BRIDGE_PARENT <- "syn12617210"
DEIDENTIFIED_DATA_OFFSET <- "syn18637903"

read_syn_csv <- function(syn_id) {
  f <- synGet(syn_id)
  df <- read_csv(f$path)
  return(df)
}

read_syn_table <- function(syn_id) {
  q <- synTableQuery(paste("select * from", syn_id))
  table <- q$asDataFrame() %>% 
    as_tibble() #%>% 
    #select(-ROW_ID, -ROW_VERSION)
  return(table)
}

curate_user_list <- function() {
  mjff_users <- read_syn_csv(MJFF_USERS) %>% 
    distinct(guid = as.character(user_id)) %>%
    mutate(source = "MJFF")
  rochester_users <- read_syn_csv(ROCHESTER_USERS) %>% 
    distinct(guid) %>% 
    mutate(source = "ROCHESTER")
  bridge_users <- read_syn_table(BRIDGE_USERS) %>% 
    distinct(guid) %>% 
    mutate(source = "BRIDGE")
  users <- bind_rows(mjff_users, rochester_users, bridge_users)
  users <- users %>% 
    group_by(guid) %>% 
    mutate(day_offset = round(runif(nrow(users), -10, 10))) %>% 
    ungroup()
  return(users)
}

perturb_mjff_dates <- function(users) {
  mjff <- list(
    "syn17098113"= read_syn_csv("syn17098113"),
    "syn17098115" = read_syn_csv("syn17098115"),
    "syn17098116" = read_syn_csv("syn17098116"),
    "syn17098117" = read_syn_csv("syn17098117"),
    "syn17098119" = read_syn_csv("syn17098119"),
    "syn17098120" = read_syn_csv("syn17098120"),
    "syn17098121" = read_syn_csv("syn17098121"),
    "syn17098122" = read_syn_csv("syn17098122"),
    "syn17098123" = read_syn_csv("syn17098123"),
    "syn17098124" = read_syn_csv("syn17098124"),
    "syn17098125" = read_syn_csv("syn17098125"),
    "syn17098126" = read_syn_csv("syn17098126"),
    "syn17098127" = read_syn_csv("syn17098127"),
    "syn17098128" = read_syn_csv("syn17098128"),
    "syn17098129" = read_syn_csv("syn17098129"),
    "syn17098130" = read_syn_csv("syn17098130"),
    "syn17098131" = read_syn_csv("syn17098131"),
    "syn17098132" = read_syn_csv("syn17098132"),
    "syn17098133" = read_syn_csv("syn17098133"),
    "syn17098134" = read_syn_csv("syn17098134"),
    "syn17098135" = read_syn_csv("syn17098135"),
    "syn17098136" = read_syn_csv("syn17098136"),
    "syn17098137" = read_syn_csv("syn17098137"),
    "syn17098138" = read_syn_csv("syn17098138"),
    "syn17098139" = read_syn_csv("syn17098139"),
    "syn17098140" = read_syn_csv("syn17098140"),
    "syn17098141" = read_syn_csv("syn17098141"),
    "syn17098142" = read_syn_csv("syn17098142"),
    "syn17098143" = read_syn_csv("syn17098143"),
    "syn17098144" = read_syn_csv("syn17098144"),
    "syn17098145" = read_syn_csv("syn17098145"))
  mjff_perturbed <- purrr::map(mjff, function(df) {
    perturb_dates(
      df = df,
      users = users,
      source_name = "MJFF",
      guid = "user_id")
  })
  # Error in make.names(x) : invalid multibyte string 1
  # sleep_habits <- read_syn_csv("syn17098146")
  # surgical_history <- read_syn_csv("syn17098146")
}

perturb_rochester_dates <- function(users) {
  rochester <- read_syn_csv(ROCHESTER_USERS)
  rochester_perturbed <- perturb_dates(
    df = rochester,
    users = users,
    source_name = "ROCHESTER",
    guid = "guid")
  return(rochester_perturbed)
}

perturb_bridge_dates <- function(users) {
  bridge <- list(
    "syn17015960" = read_syn_table("syn17015960"),
    "syn17015065" = read_syn_table("syn17015065"),
    "syn17014786" = read_syn_table("syn17014786"),
    "syn17014785" = read_syn_table("syn17014785"),
    "syn17014784" = read_syn_table("syn17014784"),
    "syn17014782" = read_syn_table("syn17014782"),
    "syn17014783" = read_syn_table("syn17014783"),
    "syn17014781" = read_syn_table("syn17014781"),
    "syn17014780" = read_syn_table("syn17014780"),
    "syn17014779" = read_syn_table("syn17014779"),
    "syn17014778" = read_syn_table("syn17014778"),
    "syn17014777" = read_syn_table("syn17014777"),
    "syn17014776" = read_syn_table("syn17014776"),
    "syn17014775" = read_syn_table("syn17014775"))
  bridge_perturbed <- purrr::map(bridge, function(df) {
    df <- df %>%
      mutate(uploadDate = lubridate::as_date(uploadDate),
             createdOn = lubridate::as_datetime(createdOn / 1000))
    if (has_name(df, "metadata.startDate")) {
      df <- df %>% 
        mutate(metadata.startDate = lubridate::as_datetime(metadata.startDate / 1000))
    }
    if (has_name(df, "metadata.endDate")) {
      df <- df %>% 
        mutate(metadata.endDate = lubridate::as_datetime(metadata.endDate / 1000))
    }
    perturb_dates(
      df = df,
      users = users,
      source_name = "BRIDGE",
      guid = "externalId")
    })
  return(bridge_perturbed)
}

perturb_dates <- function(df, users, source_name, guid) {
  if (nrow(df) == 0) {
    return(df)
  }
  df[[guid]] <- as.character(df[[guid]])
  df_with_offsets <- users %>% 
    filter(source == source_name) %>% 
    select(guid, day_offset)
  df_with_offsets <- as_tibble(
    merge(df, df_with_offsets, by.x=guid, by.y="guid", all.x = T))
  df_offsets <- lubridate::days(df_with_offsets$day_offset)
  df_dates_perturbed <- df_with_offsets %>% 
    select_if(lubridate::is.timepoint) %>% 
    purrr::map_dfc(function(col) {
      col + df_offsets
    })
  df_perturbed <- df_with_offsets %>% 
    select_if(~ !lubridate::is.timepoint(.)) %>% 
    bind_cols(df_dates_perturbed) %>% 
    select(-day_offset)
  return(df_perturbed)
}

store_rochester_perturbed <- function(rochester_dataset) {
  fname <- "deidentified_exported_records.csv"
  write_csv(rochester_dataset, fname)
  f <- synapser::File(fname, parent = ROCHESTER_PARENT)
  synStore(f)
}

store_mjff_perturbed <- function(mjff_dataset) {
  purrr::map2(names(mjff_dataset), mjff_dataset, function(.x, .y) {
    file_info <- synGet(.x, downloadFile = FALSE)
    fname <- paste0("deidentified_", file_info$properties$name)
    write_csv(.y, fname)
    f <- synapser::File(fname, parent = MJFF_PARENT)
    synStore(f)
  })
}

store_bridge_perturbed <- function(bridge_dataset) {
  purrr::map2(names(bridge_dataset), bridge_dataset, function(.x, .y) {
    table_info <- synGet(.x)
    tname <- paste("Deidentified",
                    table_info$properties$name)
    table_cols <- synGetTableColumns(.x)$asList()
    schema <- Schema(name = tname, columns = table_cols, parent = BRIDGE_PARENT)
    synStore(Table(schema, .y))
  })
}

update_user_list <- function(users) {
  existing_users <- read_syn_table(DEIDENTIFIED_DATA_OFFSET) %>%
    select(-ROW_ID, -ROW_VERSION)
  new_users <- anti_join(users, existing_users, by = "guid")
  if (nrow(new_users) > 0) {
    updated_table <- synStore(Table(DEIDENTIFIED_DATA_OFFSET, new_users))
    all_users <- read_syn_table(updated_table$tableId)
  } else {
    all_users <- existing_users
  }
  return(all_users)
}

main <- function() {
  synLogin()
  users <- curate_user_list() %>% 
    update_user_list()
  rochester_dataset <- perturb_rochester_dates(users)
  mjff_dataset <- perturb_mjff_dates(users)
  bridge_dataset <- perturb_bridge_dates(users)
  store_rochester_perturbed(rochester_dataset)
  store_mjff_perturbed(mjff_dataset)
  store_bridge_perturbed(bridge_dataset)
}

#main()