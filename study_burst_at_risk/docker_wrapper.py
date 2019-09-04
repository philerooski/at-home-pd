import argparse
import requests
import json
import os


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapseUsername")
    parser.add_argument("--synapsePassword")
    args = parser.parse_args()
    return(args)


def main():
    args = read_args()
    os.system("docker run --rm -e synapseUsername={} -e synapsePassword={} "
              "philsnyder/at-home-pd:tidyverse "
              "Rscript /root/at-home-pd/study_burst_at_risk/study_burst_at_risk.R".format(
                  args.synapseUsername, args.synapsePassword))


if __name__ == "__main__":
    main()
