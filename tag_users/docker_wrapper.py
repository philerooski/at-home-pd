import argparse
import requests
import json
import os


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapseUsername")
    parser.add_argument("--synapsePassword")
    parser.add_argument("--bridgeUsername")
    parser.add_argument("--bridgePassword")
    args = parser.parse_args()
    return(args)


def main():
    args = read_args()
    os.system("docker run --rm -e synapseUsername={} -e synapsePassword={} "
              "-e bridgeUsername={} -e bridgePassword={} "
              "philsnyder/at-home-pd:tag_users".format(
                  args.synapseUsername, args.synapsePassword,
                  args.bridgeUsername, args.bridgePassword))


if __name__ == "__main__":
    main()
