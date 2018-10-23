import synapseclient as sc


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
        "syn12514611": "syn17014786"} # WalkAndBalance-v1
PARTICIPANT_CREATION_TABLE = "syn16786935"


def get_relevant_external_ids(syn):
    participants = syn.tableQuery(
            "select * from {}".format(PARTICIPANT_CREATION_TABLE)).asDataFrame()
    is_relevant = participants.status.apply(lambda s : s.startswith("Success"))
    relevant_participants = participants.loc[is_relevant]
    return relevant_participants.guid.unique()


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
        # TODO: replace file handles with copies
        if len(new_records):
            new_target_table = sc.Table(target, new_records.values)
            syn.store(new_target_table)



def main():
    syn = sc.login()
    relevant_external_ids = get_relevant_external_ids(syn)
    update_tables(syn, relevant_external_ids)


if __name__ == "__main__":
    main()
