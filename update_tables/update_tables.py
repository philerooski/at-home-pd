import os
import synapseclient as sc
import synapsebridgehelpers


TABLE_MAPPING = {
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
        "syn12492996": "syn17015960", # Health Data Summary Table
        "syn17022539": "syn20712373"} # PassiveGait-v1

def get_env_var_credentials():
    credentials = {}
    credentials['synapseUsername'] = os.getenv('synapseUsername')
    credentials['synapsePassword'] = os.getenv('synapsePassword')
    credentials['participantsTable'] = os.getenv('participantsTable')
    credentials['substudy'] = os.getenv('substudy')
    return credentials


def get_relevant_healthcodes(syn, participants_table, substudy):
    relevant_healthcodes = syn.tableQuery(
            f"SELECT distinct healthCode FROM {participants_table} \
            where substudyMemberships like '%{substudy}%'").asDataFrame()
    relevant_healthcodes = list(relevant_healthcodes.healthCode)
    return(relevant_healthcodes)


def main():
    credentials = get_env_var_credentials()
    syn = sc.login(credentials["synapseUsername"],
                   credentials["synapsePassword"])
    relevant_healthcodes = get_relevant_healthcodes(
            syn,
            participants_table = credentials["participantsTable"],
            substudy = credentials["substudy"])
    synapsebridgehelpers.export_tables(
            syn = syn,
            table_mapping = TABLE_MAPPING,
            identifier_col = "healthCode",
            identifier = relevant_healthcodes)

if __name__ == "__main__":
    main()
