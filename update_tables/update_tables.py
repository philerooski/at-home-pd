import synapseclient as sc
import synapseutils as su
import pandas as pd
import numpy as np
import os


TABLE_MAPPINGS = {
        "syn17013485": "syn17015065", # Engagement-v1
        "syn16995608": "syn17014775", # PassiveDisplacement-v4
        "syn16982909": "syn17014776", # PassiveDisplacement-v2
        "syn15673384": "syn17014777", # Medication-v3
        "syn15673383": "syn17014778", # StudyBurst-v1
        "syn15673382": "syn17014779", # Triggers-v3
        "syn15673381": "syn17014780", # Tapping-v4
        "syn15673380": "syn17014781", # Symptoms-v3
        "syn15673379": "syn17014782", # Demographics-v1
        "syn15673377": "syn17014783", # StudyBurstReminder-v1
        "syn15664831": "syn17014784", # Motivation-v1
        "syn12977322": "syn17014785", # Tremor-v3
        "syn12514611": "syn17014786", # WalkAndBalance-v1
        "syn12492996": "syn17015960"} # Health Data Summary Table
PARTICIPANT_CREATION_TABLE = "syn16786935"


def get_env_var_credentials():
    credentials = {}
    credentials['synapseUsername'] = os.getenv('synapseUsername')
    credentials['synapsePassword'] = os.getenv('synapsePassword')
    return credentials


def get_relevant_external_ids(syn):
    participants = syn.tableQuery(
            "select * from {}".format(PARTICIPANT_CREATION_TABLE)).asDataFrame()
    is_relevant = participants.status.apply(lambda s : s.startswith("Success"))
    relevant_participants = participants.loc[is_relevant]
    return relevant_participants.guid.unique()


def copy_file_handles(syn, new_records, source):
    cols = [c for c in syn.getColumns(source)]
    for c in cols:
        if c['columnType'] == 'FILEHANDLEID':
            fhids_to_copy = new_records[c['name']].dropna().astype(int).tolist()
            new_fhids = []
            for i in range(0, len(fhids_to_copy), 100):
                fhids_to_copy_i = fhids_to_copy[i:i+100]
                new_fhids_i = su.copyFileHandles(
                        syn = syn,
                        fileHandles = fhids_to_copy_i,
                        associateObjectTypes = ["TableEntity"] * len(fhids_to_copy_i),
                        associateObjectIds = [source] * len(fhids_to_copy_i),
                        contentTypes = ["application/json"] * len(fhids_to_copy_i),
                        fileNames = [None] * len(fhids_to_copy_i))
                for j in [int(i['newFileHandle']['id']) for i in new_fhids_i['copyResults']]:
                    new_fhids.append(j)
            new_fhids = pd.DataFrame(
                    {c['name']: fhids_to_copy,
                     "new_fhids": new_fhids})
            new_records = new_records.merge(new_fhids, how='left', on=c['name'])
            new_records[c['name']] = new_records['new_fhids']
            new_records = new_records.drop("new_fhids", axis = 1)
            new_records = new_records.drop_duplicates(subset = "recordId")
        elif c['columnType'] not in ["INTEGER", "DOUBLE"]:
            new_records[c['name']] = [
                    None if pd.isnull(i) else i for i in new_records[c['name']]]
    return new_records

def parse_float_to_int(i):
    str_i = str(i)
    if "nan" == str_i:
        str_i = None
    elif str_i.endswith(".0"):
        str_i = str_i[:-2]
    return(str_i)

def sanitize_table(syn, target, records):
    cols = syn.getTableColumns(target)
    for c in cols:
        if c['columnType'] == 'STRING':
            if ('timezone' in c['name'].lower() and
                type(records[c['name']].iloc[0]) is np.float64):
                records[c['name']] = list(map(parse_float_to_int, records[c['name']]))
        if ((c['columnType'] == 'INTEGER' or c['columnType'] == "DATE") and
            len(records[c['name']]) and
            type(records[c['name']].iloc[0]) is np.float64):
            records[c['name']] = list(map(parse_float_to_int, records[c['name']]))
        if c['columnType'] == 'FILEHANDLEID':
            records[c['name']] = list(map(parse_float_to_int, records[c['name']]))
    return records


def update_tables(syn, relevant_external_ids):
    for source, target in TABLE_MAPPINGS.items():
        external_ids_str = "('{}')".format("', '".join(relevant_external_ids))
        source_table = syn.tableQuery(
                "SELECT * FROM {} WHERE externalId IN {}".format(
                    source, external_ids_str))
        source_table = source_table.asDataFrame().set_index("recordId", drop=False)
        try:
            target_table = syn.tableQuery("select * from {}".format(target))
        except sc.exceptions.SynapseTimeoutError:
            print("{} failed to update because it could not "
                  "be fetched.".format(target))
            continue
        target_table = target_table.asDataFrame().set_index("recordId", drop=False)
        new_records = source_table.loc[
                source_table.index.difference(target_table.index)]
        if len(new_records): # new records found from the relevant external ids
            new_records = copy_file_handles(syn, new_records, source)
            new_records = sanitize_table(syn, target, new_records)
            new_target_table = sc.Table(target, new_records.values.tolist())
            try:
                syn.store(new_target_table, used = source)
            except Exception as e:
                print(source)
                print(new_records)
                raise(e)


def main():
    credentials = get_env_var_credentials()
    syn = sc.login(email = credentials['synapseUsername'],
                   password = credentials['synapsePassword'])
    relevant_external_ids = get_relevant_external_ids(syn)
    update_tables(syn, relevant_external_ids)


if __name__ == "__main__":
    main()
