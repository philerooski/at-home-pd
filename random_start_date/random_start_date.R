library(synapser)
library(tidyverse)

MJFF_USERS <- "syn18680002"
ROCHESTER_USERS <- "syn17051543"
BRIDGE_USERS <- "syn16786935"
ROCHESTER_PARENT <- "syn18637131"
MJFF_PARENT <- "syn18637133"
BRIDGE_PARENT <- "syn12617210"
DEIDENTIFIED_DATA_OFFSET <- "syn18637903"
MJFF_ORIGINALS_W_IDENTIFIER <- "syn18678038"
BRIDGE_MAPPING <- list(
    "syn17015960" = "syn18681888",
    "syn17015065" = "syn18681890",
    "syn17014786" = "syn18681891",
    "syn17014785" = "syn18681893",
    "syn17014784" = "syn18681894",
    "syn17014783" = "syn18681898",
    "syn17014781" = "syn18681899",
    "syn17014780" = "syn18681900",
    "syn17014779" = "syn18681901",
    "syn17014778" = "syn18681902",
    "syn17014777" = "syn18681903",
    "syn17014776" = "syn18681904",
    "syn17014775" = "syn18681905",
    "syn17014782" = "syn18683083")

read_syn_csv <- function(syn_id, encoding = "UTF-8") {
  f <- synGet(syn_id)
  df <- read_csv(f$path, locale = locale(encoding = encoding))
  return(df)
}

read_syn_table <- function(syn_id) {
  q <- synTableQuery(paste("select * from", syn_id))
  table <- q$asDataFrame() %>% 
    as_tibble() %>% 
    select(-ROW_ID, -ROW_VERSION)
  return(table)
}

curate_user_list <- function() {
  mjff_users <- read_syn_csv(MJFF_USERS) %>% 
    distinct(guid) %>%
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
    mutate(day_offset = round(runif(1, -10, 10))) %>% 
    ungroup()
  return(users)
}

perturb_mjff_dates <- function(users) {
  mjff <- synGetChildren("syn18678038")$asList()
  names(mjff) <- purrr::map(mjff, ~ .$id)
  mjff <- purrr::map(mjff, ~ read_syn_csv(.$id))
  mjff$syn18680002 <- NULL # users.csv
  date_cols <- c("study_visit_start_date", "first_answer_created_at",
                 "last_answer_created_at", "last_answer_modified_at",
                 paste("1.1.0 When did you start taking prescription medication",
                        "to treat your Parkinson's disease?"),
                 paste("1.1 When were you first diagnosed with Parkinson's disease",
                       "or parkinsonism (to the best of your memory)?"))
  mjff_dates <- mjff %>% 
    purrr::map(function(df) {
      df <- df %>% 
        select_if(names(df) %in% date_cols) %>% 
        purrr::map_dfc(lubridate::as_datetime)
      return(df)
    })
  mjff <- purrr::map2(mjff, mjff_dates, function(df, df_dates) {
      df <- df %>% 
        select_if(!(names(df) %in% date_cols)) %>% 
        bind_cols(df_dates)
      return(df)
    })
  mjff_perturbed <- purrr::map(mjff, function(df) {
    col_order <- names(df)
    perturbed_dates <- perturb_dates(
      df = df,
      users = users,
      source_name = "MJFF",
      guid = "guid",
      date_cols = date_cols)
    return(perturbed_dates[col_order])
  })
}

perturb_rochester_dates <- function(users) {
  rochester <- read_syn_csv(ROCHESTER_USERS)
  date_cols <- c("assessdate_fall", "assessdate_fall_m12", "bl_partburdenblvisitdt",
                 "determinefall_visitdt", "compliance_dttm", "concldttm", "wddt",
                 "irbapprovedt", "subjsigdt", "start_dt", "inexdttm", "demo_dttm",
                 "dob", "screenorientdttm", "phoneorientdttm", "onsetdt", "mdsupdrs_dttm",
                 "moca_dttm", "mseadl", "cgi_dttm", "determinefall_visitdt",
                 "bl_partburdenblvisitdt", "compliance_dttm", "visstatdttm",
                 "invsigdttm", "stop_dt", "notifdt", "eventdt", "reslvdt",
                 "partburdenv1visitdt")
  col_order <- names(rochester)
  rochester_perturbed <- perturb_dates(
    df = rochester,
    users = users,
    source_name = "ROCHESTER",
    guid = "guid",
    date_cols = date_cols)
  return(rochester_perturbed[col_order])
}

perturb_bridge_dates <- function(users, table_mapping = NULL) {
  bridge_tables <- c("syn17015960","syn17015065","syn17014786",
                     "syn17014785","syn17014784","syn17014782",
                     "syn17014783","syn17014781","syn17014780",
                     "syn17014779","syn17014778","syn17014777",
                     "syn17014776","syn17014775")
  expected_tables <- c(bridge_tables, unlist(BRIDGE_MAPPING, use.names=F),
                       "syn18693245", "syn16784393", "syn16786935", "syn18637903")
  actual_tables <- synGetChildren(BRIDGE_PARENT, includeTypes=list("table"))$asList() %>% 
    purrr::map(~ .$id) %>% 
    unlist()
  unexpected_tables <- setdiff(actual_tables, expected_tables)
  if (length(unexpected_tables)) {
    stop(paste("Unexpected table(s) found in the AT-HOME PD project:",
               stringr::str_c(unexpected_tables, collapse=", "))) 
  }
  bridge <- purrr::map(bridge_tables, read_syn_table)
  names(bridge) <- bridge_tables
  if (!is.null(table_mapping)) {
    deidentified_bridge <- purrr::map(table_mapping, read_syn_table)
    bridge_diff <- purrr::map(names(bridge), function(source_id) {
      source_table <- bridge[[source_id]] %>% 
        mutate(recordId = as.character(recordId))
      target_table <- deidentified_bridge[[source_id]] %>% 
        mutate(recordId = as.character(recordId))
      new_records <- anti_join(source_table, target_table, by = "recordId")
      return(new_records)
    })
  names(bridge_diff) <- names(bridge)
  }
  bridge_perturbed <- purrr::map(bridge_diff, function(df) {
    col_order <- names(df)
    df <- df %>%
      mutate(uploadDate = lubridate::as_date(uploadDate),
             createdOn = lubridate::as_datetime(createdOn))
    if (has_name(df, "metadata.startDate")) {
      df <- df %>% 
        mutate(metadata.startDate = lubridate::as_datetime(metadata.startDate))
    }
    if (has_name(df, "metadata.endDate")) {
      df <- df %>% 
        mutate(metadata.endDate = lubridate::as_datetime(metadata.endDate))
    }
    bridge_perturbed <- perturb_dates(
      df = df,
      users = users,
      source_name = "BRIDGE",
      guid = "externalId")
    return(bridge_perturbed[col_order])
    })
  return(bridge_perturbed)
}

perturb_dates <- function(df, users, source_name, guid, date_cols=NULL) {
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
  if (is.null(date_cols)) {
    df_dates_perturbed <- df_with_offsets %>% 
      select_if(lubridate::is.timepoint)
    df_perturbed <- df_with_offsets %>% 
      select_if(~ !lubridate::is.timepoint(.))
  } else {
    df_dates_perturbed <- df_with_offsets %>% 
      select_if(names(.) %in% date_cols)
    df_perturbed <- df_with_offsets %>% 
      select_if(!(names(.) %in% date_cols))
  }
  df_dates_perturbed <- purrr::map_dfc(df_dates_perturbed, function(col) {
    if (typeof(col) == "logical") { # empty col
      return(col)
    }
    return(col + df_offsets)
  })
  df_perturbed <- df_perturbed %>%
    bind_cols(df_dates_perturbed) %>% 
    select(-day_offset)
  return(df_perturbed)
}

store_rochester_perturbed <- function(rochester_dataset) {
  fname <- "deidentified_exported_records.csv"
  write_csv(rochester_dataset, fname)
  f <- synapser::File(fname, parent = ROCHESTER_PARENT)
  synStore(f, used = list(ROCHESTER_USERS))
}

store_mjff_perturbed <- function(mjff_dataset, table_mapping=NULL) {
  purrr::map2(names(mjff_dataset), mjff_dataset, function(.x, .y) {
    file_info <- synGet(.x, downloadFile = FALSE)
    fname <- paste0("deidentified_", file_info$properties$name)
    write_csv(.y, fname)
    f <- synapser::File(fname, parent = MJFF_PARENT)
    synStore(f, used = list(.x))
  })
}

store_bridge_perturbed <- function(bridge_dataset, table_mapping=NULL) {
  if (!is.null(table_mapping)) {
    purrr::map2(bridge_dataset, names(bridge_dataset), function(df, source) {
      if (nrow(df) > 0) {
        target <- table_mapping[[source]]
        synStore(Table(target, df), used = list(source))
      }
    })
  }
  else {
    purrr::map2(names(bridge_dataset), bridge_dataset, function(.x, .y) {
      table_info <- synGet(.x)
      tname <- paste("Deidentified",
                      table_info$properties$name)
      table_cols <- synGetTableColumns(.x)$asList()
      schema <- Schema(name = tname, columns = table_cols, parent = BRIDGE_PARENT)
      synStore(Table(schema, .y), used = list(.x))
    })
  }
}

update_user_list <- function(users) {
  existing_users <- read_syn_table(DEIDENTIFIED_DATA_OFFSET) %>%
    mutate(guid = as.character(guid))
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
  bridge_dataset <- perturb_bridge_dates(users, table_mapping = BRIDGE_MAPPING)
  store_rochester_perturbed(rochester_dataset)
  store_mjff_perturbed(mjff_dataset)
  store_bridge_perturbed(bridge_dataset, table_mapping = BRIDGE_MAPPING)
}

main()
