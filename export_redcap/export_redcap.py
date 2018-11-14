import argparse
import redcap
import synapseclient as sc

SYNAPSE_PARENT = "syn16809549"

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redcap-url", help="Redcap API url")
    parser.add_argument("--redcap-token", help="RedCap API Token")
    args = parser.parse_args()
    return(args)


def filter_identifiers(records):
    identifiers = [
            'subj_name', 'phone', 'emergency_contact', 'emerphone', 'email',
            'street1', 'street2', 'city', 'state', 'zipcode', 'dob']
    records = records.drop(identifiers, axis = 1)
    return records


def store_to_synapse(records):
    syn = sc.login()
    records.to_csv("exported_records.csv", index = False)
    f = sc.File("exported_records.csv", parent = SYNAPSE_PARENT)
    syn.store(f)


def main():
    args = read_args()
    proj = redcap.Project(url = args.redcap_url, token = args.redcap_token)
    exported_records = proj.export_records(raw_or_label = "label", format = "df")
    exported_records = filter_identifiers(exported_records)
    store_to_synapse(exported_records)


if __name__ == "__main__":
    main()
