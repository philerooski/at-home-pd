import argparse
import pandas as pd
import synapseclient as sc
import bridgeclient as bc


BRIDGE_STUDY = "sage-mpower-2"
AT_HOME_PD_USER_LIST = "syn16786935"
HEALTH_DATA_SUMMARY_TABLE = "syn12492996"
OUTPUT_PARENT = "syn7222412"
LAUNCH_TIME = 1537257600 * 1000


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bridgeUsername", required = True)
    parser.add_argument("--bridgePassword", required = True)
    parser.add_argument("--synapseUsername", required = True)
    parser.add_argument("--synapsePassword", required = True)
    args = parser.parse_args()
    return(args)


def tag_users(syn, bridge):
    all_participants = syn.tableQuery("select * from {}".format(
        HEALTH_DATA_SUMMARY_TABLE)).asDataFrame()
    all_participants = all_participants.drop_duplicates(subset = "healthCode")
    bridge_participants = bridge.getParticipants()
    bridge_metadata = bridge_participants.id.apply(
            lambda i : bridge.getParticipantMetaData(i))
    bridge_participants['healthCode'] = [m['healthCode'] for m in bridge_metadata]
    at_home_pd_users = get_at_home_pd_users(syn)
    # is test user
    hc_not_in_bridge = all_participants.healthCode.apply(
            lambda hc : hc not in bridge_participants.healthCode.values)
    # is test user
    externalid_not_in_at_home_pd = all_participants.externalId.apply(
            lambda eid : pd.notnull(eid) and
                         eid not in at_home_pd_users.guid.values)
    # is test user
    test_groups = ['test_no_consent', 'test_user']
    in_test_group = all_participants.dataGroups.apply(
            lambda g : isinstance(g, str) and
                       any([i in g.split(",") for i in test_groups]))
    # is test user
    joined_before_study_launch = all_participants.createdOn.apply(
            lambda d : d < LAUNCH_TIME)
    is_test_user = [any(b) for b in zip(
        hc_not_in_bridge, externalid_not_in_at_home_pd,
        in_test_group, joined_before_study_launch)]
    all_participants['userType'] = [
            "test" if b else "actual" for b in is_test_user]
    all_participants['atHomePD'] = all_participants.externalId.apply(
            lambda eid: eid in at_home_pd_users.guid.values)
    return(all_participants)


def get_at_home_pd_users(syn):
    users = syn.tableQuery(
            "select * from {}".format(AT_HOME_PD_USER_LIST)).asDataFrame()
    users = users[["Success" in s for s in users.status]]
    return users


def push_to_synapse(syn, all_participants):
    result = all_participants[["healthCode", "userType", "atHomePD"]]
    fname = "mpower2_healthcode_categorizations.csv"
    result.to_csv(fname, index = False)
    f = sc.File(fname, parent = OUTPUT_PARENT)
    syn.store(f)


def main():
    args = read_args()
    bridge = bc.bridgeConnector(args.bridgeUsername,
                                args.bridgePassword,
                                study = BRIDGE_STUDY)
    syn = sc.login(args.synapseUsername, args.synapsePassword)
    all_participants = tag_users(syn, bridge)
    push_to_synapse(syn, all_participants)


if __name__ == "__main__":
    main()
