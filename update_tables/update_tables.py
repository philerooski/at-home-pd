import synapseclient as sc
import synapseutils as su
import pandas as pd
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
            new_fhids = su.copyFileHandles(
                    syn = syn,
                    fileHandles = fhids_to_copy,
                    associateObjectTypes = ["TableEntity"] * len(fhids_to_copy),
                    associateObjectIds = [source] * len(fhids_to_copy),
                    contentTypes = ["application/json"] * len(fhids_to_copy),
                    fileNames = [None] * len(fhids_to_copy))
            new_fhids = pd.DataFrame(
                    {c['name']: fhids_to_copy,
                     "new_fhids": [int(i['newFileHandle']['id']) for i in
                                        new_fhids['copyResults']]})
            new_records = new_records.merge(new_fhids, how='left', on=c['name'])
            new_records[c['name']] = new_records['new_fhids']
            new_records = new_records.drop("new_fhids", axis = 1)
            new_records = new_records.drop_duplicates(subset = "recordId")
        elif c['columnType'] not in ["INTEGER", "DOUBLE"]:
            new_records[c['name']] = [
                    None if pd.isnull(i) else i for i in new_records[c['name']]]
    return new_records


def update_tables(syn, relevant_external_ids):
    for source, target in TABLE_MAPPINGS.items():
        external_ids_str = "('{}')".format("', '".join(relevant_external_ids))
        source_table = syn.tableQuery(
                "SELECT * FROM {} WHERE externalId IN {}".format(
                    source, external_ids_str))
        source_table = source_table.asDataFrame().set_index("recordId", drop=False)
        target_table = syn.tableQuery("select * from {}".format(target))
        target_table = target_table.asDataFrame().set_index("recordId", drop=False)
        new_records = source_table.loc[
                source_table.index.difference(target_table.index)]
        if len(new_records): # new records found from the relevant external ids
            new_records = copy_file_handles(syn, new_records, source)
            new_target_table = sc.Table(target, new_records.values.tolist())
            syn.store(new_target_table, used = source)


def main():
    credentials = get_env_var_credentials()
    syn = sc.login(email = credentials['synapseUsername'],
                   password = credentials['synapsePassword'])
    relevant_external_ids = get_relevant_external_ids(syn)
    update_tables(syn, relevant_external_ids)


if __name__ == "__main__":
    main()
