import sys
import os
import pandas as pd
import requests

if "data_temp" not in os.listdir("."):
    os.mkdir("data_temp")

flow = sys.argv[1]

def download_file(filename):
    url = "http://www.mdic.gov.br/balanca/bd/comexstat-bd/mun/{}".format(filename)
    print("Downloading from {}...".format(url))
    r = requests.get(url)
    with open("./data_temp/" + filename, "w") as file:
        file.write(r.text)
    print("Downloaded {}".format(filename))


if sys.argv[1] in ["EXP","IMP"]:
    year = sys.argv[2]
    filename = "{}_{}_MUN.csv".format(flow,year)
    if filename in os.listdir("./data_temp"):
        confirmation = input("The file {} already exists, do you want to replace it? [yes/no]: ".format(filename))
        if confirmation == "yes":
            download_file(filename)
    else:
        download_file(filename)

elif sys.argv[1] == 'all':
    for flow in ["EXP","IMP"]:
        for year in [str(x) for x in range(1997,2020)]:
            filename = "{}_{}_MUN.csv".format(flow,year)
            if filename not in os.listdir("./data_temp"):
                download_file(filename)
            else:
                download_file(filename)
                #confirmation = input("The file {} already exists, do you want to replace it? [yes/no]: ".format(filename))
                #if confirmation == "yes":
                #    download_file(filename)

