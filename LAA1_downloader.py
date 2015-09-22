# -*- coding: utf-8 -*-
#
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

# url = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/410395/SFR36_2014_LA_tables_revised.xlsx"
# output_path = "" 
# sheet = "LAA1"
# required_indicators = ["2011", "2012", "2013", "2014"]


def download(url, sheet, reqFields, outPath):
    yearReq = reqFields
    dName = outPath

    col = ['ecode', 'name', 'year', 'rate']
  
    try:
        socket = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        sys.exit('excel download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        sys.exit('excel download URLError = ' + str(e.args))
    except Exception:
        print ('excel file download error')
        import traceback
        sys.exit('generic exception: ' + traceback.format_exc())
    
    #operate this excel file
    xd = pd.ExcelFile(socket)
    df = xd.parse(sheet)
    
    print ('indicator checking------')
    for i in range(df.shape[0]):
        yearCol = []
        for k in yearReq:
            kk = []
            for j in range(df.shape[1]):
                if df.iloc[i,j]==(int(k) or k):
                    kk.append(j)
                    restartIndex = i + 1

            if len(kk)==2:
                yearCol.append(max(kk))
        
        #print ('checking row ' + i)
        
        if len(yearCol)==len(yearReq):
            break
    
    if len(yearCol)!=len(yearReq):
        sys.exit("Requested data " + str(yearReq).strip('[]') + " don't match the excel file. Please check the file at: " + url)
    
    raw_data = {}
    for j in col:
        raw_data[j] = []
    
    print ('data reading------')
    for i in range(restartIndex, df.shape[0]):
        if re.match(r'^\d{3}$', str(df.iloc[i, 0])):
            ii = 0
            for j in range(len(yearCol)):
                raw_data[col[0]].append(df.iloc[i, 0])
                raw_data[col[1]].append(df.iloc[i, 1])
                raw_data[col[2]].append(yearReq[ii])
                raw_data[col[3]].append(df.iloc[i, yearCol[ii]])
                ii += 1
    
        #print ('reading row ' + i)
    
    #save csv file
    print ('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns = col)
    dfw.to_csv(dName, index = False)
    print('Requested data has been extracted and saved as ' + dName)
    print("finished")

parser = argparse.ArgumentParser(description='Extract online Excel file LAA1 to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_LAA1.json", action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig: 
    obj = {"url":"https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/410395/SFR36_2014_LA_tables_revised.xlsx", 
           "outPath":"tempLAA1.csv",
           "sheet":"LAA1",
           "reqFields":["2011", "2012", "2013", "2014"]
           }

    with open("config_LAA1.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_LAA1.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)
    print ("read config file")

download(oConfig["url"], oConfig["sheet"], oConfig["reqFields"], oConfig["outPath"])
