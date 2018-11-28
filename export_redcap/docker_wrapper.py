import argparse
import requests
import json
import os


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapseUsername")
    parser.add_argument("--synapsePassword")
    parser.add_argument("--redcapURL")
    parser.add_argument("--redcapToken")
    args = parser.parse_args()
    return(args)


def main():
    args = read_args()
    os.system("docker run --rm -e synapseUsername={} -e synapsePassword={} "
              "-e redcapURL={} -e redcapToken={} "
              "philsnyder/at-home-pd:export_redcap".format(
                  args.synapseUsername, args.synapsePassword,
                  args.redcapURL, args.redcapToken))


if __name__ == "__main__":
    main()
