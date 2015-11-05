# -*- coding: utf-8 -*-

"""
LAA1_downloader.py
Created on Fri Sep 18 14:05:54 2015

@author: G
"""

import sys
import urllib
import pandas as pd
import re
import argparse
import json
import datetime
import hashlib

# url = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/410395/SFR36_2014_LA_tables_revised.xlsx"
# output_path = "tempLAA1.csv"
# sheet = "LAA1"
# required_indicators = ["2011", "2012", "2013", "2014"]


def download(url, sheet, reqFields, outPath):
    yearReq = reqFields
    dName = outPath

    col = ['ecode', 'name', 'year', 'rate', 'pkey']

    # open url
    socket = openurl(url)

    #operate this excel file
    logfile.write(str(now()) + ' excel file loading\n')
    print('excel file loading------')
    xd = pd.ExcelFile(socket)
    df = xd.parse(sheet)

    # indicator checking
    logfile.write(str(now()) + ' indicator checking\n')
    print('indicator checking------')
    for i in range(df.shape[0]):
        yearCol = []
        for k in yearReq:
            kk = []
            for j in range(df.shape[1]):
                if df.iloc[i,j] == (int(k) or k):
                    kk.append(j)
                    restartIndex = i + 1

            if len(kk) == 2:
                yearCol.append(max(kk))
        
        if len(yearCol) == len(yearReq):
            break
    
    if len(yearCol) != len(yearReq):
        errfile.write(str(now()) + " Requested data " + str(yearReq).strip(
            '[]') + " don't match the excel file. Please check the file at: " + str(url) + " . End progress\n")
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit("Requested data " + str(yearReq).strip('[]') + " don't match the excel file. Please check the file at: " + url)
    
    raw_data = {}
    for j in col:
        raw_data[j] = []

    # data reading
    logfile.write(str(now()) + ' data reading\n')
    print('data reading------')
    for i in range(restartIndex, df.shape[0]):
        if re.match(r'^\d{3}$', str(df.iloc[i, 0])):
            ii = 0
            for j in range(len(yearCol)):
                raw_data[col[0]].append(df.iloc[i, 0])
                raw_data[col[1]].append(df.iloc[i, 1])
                raw_data[col[2]].append(yearReq[ii])
                raw_data[col[3]].append(df.iloc[i, yearCol[ii]])
                ii += 1
    logfile.write(str(now()) + ' data reading end\n')
    print('data reading end------')

    # create primary key by md5 for each row
    logfile.write(str(now()) + ' create primary key\n')
    print('create primary key------')
    keyCol = [0, 2]
    raw_data[col[-1]] = fpkey(raw_data, col, keyCol)
    logfile.write(str(now()) + ' create primary key end\n')
    print('create primary key end------')

    #save csv file
    logfile.write(str(now()) + ' writing to file\n')
    print('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns=col)
    dfw.to_csv(dName, index=False)
    logfile.write(str(now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now()) + ' finished\n')
    print("finished")

def openurl(url):
    try:
        socket = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        errfile.write(str(now()) + ' file download HTTPError is ' + str(e.code) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        errfile.write(str(now()) + ' file download URLError is ' + str(e.args) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download URLError = ' + str(e.args))
    except Exception:
        print('file download error')
        import traceback
        errfile.write(str(now()) + ' generic exception: ' + str(traceback.format_exc()) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('generic exception: ' + traceback.format_exc())

    return socket

def fpkey(data, col, keyCol):
    mystring = ''
    pkey = []
    for i in range(len(data[col[0]])):
        for j in keyCol:
            mystring += str(data[col[j]][i])
        mymd5 = hashlib.md5(mystring.encode()).hexdigest()
        pkey.append(mymd5)

    return pkey

def now():
    return datetime.datetime.now()


parser = argparse.ArgumentParser(description='Extract online Children in Care Excel file LAA1 to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_LAA1.json", action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig: 
    obj = {"url": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/410395/SFR36_2014_LA_tables_revised.xlsx",
           "outPath": "tempLAA1.csv",
           "sheet": "LAA1",
           "reqFields": ["2011", "2012", "2013", "2014"]
           }

    logfile = open("log_tempLAA1.log", "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open("err_tempLAA1.err", "w")

    with open("config_tempLAA1.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempLAA1.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["sheet"], oConfig["reqFields"], oConfig["outPath"])
