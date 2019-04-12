import argparse
import redcap
import synapseclient as sc
import os

SYNAPSE_PARENT = "syn16809549"

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redcap-url", help="Redcap API url")
    parser.add_argument("--redcap-token", help="RedCap API Token")
    parser.add_argument('--synapseUsername')
    parser.add_argument('--synapsePassword')
    args = parser.parse_args()
    return(args)

def get_env_var_credentials():
    credentials = {}
    credentials['synapseUsername'] = os.getenv('synapseUsername')
    credentials['synapsePassword'] = os.getenv('synapsePassword')
    credentials['redcapURL'] = os.getenv('redcapURL')
    credentials['redcapToken'] = os.getenv('redcapToken')
    return credentials

def filter_identifiers(records):
    identifiers = [
            'subj_name', 'phone', 'emergency_contact', 'emerphone', 'email',
            'street1', 'street2', 'city', 'state', 'zipcode', 'dob',
	    'num_type']
    present_identifiers = [i for i in identifiers if i in records.columns]
    if len(present_identifiers):
        records = records.drop(present_identifiers, axis = 1)
    return records


def store_to_synapse(syn, records):
    records.to_csv("exported_records.csv")
    f = sc.File("exported_records.csv", parent = SYNAPSE_PARENT)
    syn.store(f)


def main():
    #credentials = read_args()
    credentials = get_env_var_credentials()
    syn = sc.login(credentials['synapseUsername'],
                   credentials['synapsePassword'])
    proj = redcap.Project(url = credentials['redcapURL'],
                          token = credentials['redcapToken'])
    exported_records = proj.export_records(raw_or_label = "label", format = "df")
    exported_records = filter_identifiers(exported_records)
    store_to_synapse(syn, exported_records)


if __name__ == "__main__":
    main()
