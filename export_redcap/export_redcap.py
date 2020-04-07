import argparse
import redcap
import synapseclient as sc
import os

SYNAPSE_PARENT = "syn16809549"
NON_STANDARD_FIELDS = [
        'demographics_spd_complete',
        'informed_consent_log_complete',
        'notifications_complete',
        'inclusion_exclusion_complete',
        'bl_visit_date_complete',
        'v01_visit_date_complete',
        'v02_visit_date_complete',
        'reportable_event_complete',
        'mdsupdrs_complete',
        'moca_complete',
        'prebaseline_survey_complete',
        'previsit_survey_complete',
        'compliance_assessment_complete',
        'substudy_moca_complete',
        'visit_date_spd_complete',
        'medical_history_complete',
        'moca_spd_complete',
        'pdq39_complete',
        'general_healthprop_complete',
        'psprop_complete',
        'pdprop_complete',
        'conclusion_complete',
        'visit_status_complete',
        'study_burst_reminders_complete',
        'subject_contact_information_complete',
        'concomitant_medication_log_complete',
        'enrollment_confirmation_complete',
        'participant_demographics_complete',
        'screen_televisit_orientation_checklist_complete',
        'smartphone_app_orientation_complete',
        'modified_schwab_and_england_adl_complete',
        'modified_clinical_global_impression_scale_complete',
        'determination_of_falls_complete',
        'preference_and_burden_bl_complete',
        'preference_and_burden_visit_complete',
        'smartphone_app_completion_complete',
        'inclusion_exclusion_spd_complete',
        'substudy_mdsupdrs_part_iii_complete',
        'concomitant_medications_complete',
        'participant_mdsupdrs_survey_complete',
        'mdsupdrs_physician_exam_complete',
        'patient_global_impression_scale_complete',
        'clinical_global_impression_scale_complete',
        'investigator_signature_complete']

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
            'street1', 'street2', 'city', 'state', 'zipcode', 'num_type']
    present_identifiers = [i for i in identifiers if i in records.columns]
    if len(present_identifiers):
        records = records.drop(present_identifiers, axis = 1)
    return records


def store_to_synapse(syn, records, name):
    records.to_csv(name)
    f = sc.File(name, parent = SYNAPSE_PARENT)
    syn.store(f)


def main():
    #credentials = read_args()
    credentials = get_env_var_credentials()
    syn = sc.login(credentials['synapseUsername'],
                   credentials['synapsePassword'])
    proj = redcap.Project(url = credentials['redcapURL'],
                          token = credentials['redcapToken'])
    exported_records_label = proj.export_records(
            fields = proj.field_names + NON_STANDARD_FIELDS,
            raw_or_label = "label",
            format = "df",
            export_survey_fields = True)
    exported_records_raw = proj.export_records(
            raw_or_label = "raw",
            fields = proj.field_names + NON_STANDARD_FIELDS,
            format = "df",
            export_survey_fields = True)
    exported_records_label = filter_identifiers(exported_records_label)
    exported_records_raw = filter_identifiers(exported_records_raw)
    store_to_synapse(syn, exported_records_label, "exported_records.csv")
    store_to_synapse(syn, exported_records_raw, "exported_records_raw.csv")


if __name__ == "__main__":
    main()
