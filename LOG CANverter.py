#! /usr/bin/env python3

import cantools
import os
from itertools import compress
from tqdm import tqdm
import re
import csv
from tkinter import Message, Tk
from tkinter.filedialog import askopenfilename
from tkinter.filedialog import asksaveasfilename
import time

def validate_decode():
    try:
        db.decode_message(arbitration_id, data)
        return True
    except:
        return False

arb_id_list = []
signalList = ['Time']
displaySignalList = ['Time']
signalUnit = ['s']
signalMin = [None]
signalMax = [None]
values_list = []
aggregated_values_list = []
signalactive_list = []
signals_bool = 0
dps_list = [3]
dpsbase = 3
loggingbase = 1
frequency = 100
starttime = float(0)
lastwritetime = float(0)
outputlinecount = 2

Tk().withdraw() # we don't want a full GUI, so keep the root window from appearing

logfilename = askopenfilename(title = "Select Log File",filetypes = (("LOG Files","*.log"),("all files","*.*"))) 
dbcfilename = askopenfilename(title = "Select DBC File",filetypes = (("DBC Files","*.dbc"),("all files","*.*"))) 
outputfile = asksaveasfilename(title = "Save Exported CSV File", filetypes = (("CSV Files","*.csv"),("all files","*.*")))
tempfile = outputfile + ".temp"

with open (logfilename, "r",encoding="utf8") as inputfile:
    print("Calculating Total Lines... \n")
    numlines = sum(1 for line in inputfile)
inputfile.close()

with open (logfilename, "r",encoding="utf8") as inputfile:

    with open(tempfile, "w", newline='') as logfile:
        writecsv = csv.writer(logfile, quoting=csv.QUOTE_ALL, delimiter=",")
        db = cantools.database.load_file(dbcfilename)
        raw_dbc = db.messages
        for iterable in raw_dbc:
            listmsgs = str(iterable).split(',')
            arb_id = int(listmsgs[1],0)
            arb_id_list.append(arb_id)
        arb_id_list.sort()

        for count,i in enumerate(arb_id_list):
            frameID = db.get_message_by_frame_id(arb_id_list[count])
            signalset = frameID.signals        

            if len(signalset) > 0:
                for i, iterable in enumerate(signalset):
                    if frameID.signals[i].is_multiplexer == False:
                        signalname    = str(frameID.signals[i].name)
                        modsignalname = str(frameID.signals[i].name).replace("_"," ")
                        signalunit    = frameID.signals[i].unit
                        signalcomment = frameID.signals[i].comment
                        signalminimum = frameID.signals[i].minimum
                        signalmaximum = frameID.signals[i].maximum
                        if signalcomment != None:
                            try:
                                log = int(re.findall("LOG = (d{1})",signalcomment)[0])
                            except:
                                log = loggingbase
                        else:
                            log = loggingbase
                        if log >=1:
                            signalList.append(signalname)
                            displaySignalList.append(modsignalname)
                            signalMin.append(signalminimum)
                            signalMax.append(signalmaximum)
                            if signalunit != None:
                                signalUnit.append(signalunit)
                            else:
                                signalUnit.append('')
                            if signalcomment != None:
                                try:
                                    dps = int(re.findall("DPS = (\d{2}|\d{1})",signalcomment)[0])
                                except:
                                    dps = dpsbase
                            else:
                                dps = dpsbase
                            dps_list.append(dps)

        writecsv.writerow(displaySignalList)
        writecsv.writerow(signalUnit)

        for iterable in range(len(signalList)) :
            values_list.append([])
            aggregated_values_list.append('')
            signalactive_list.append(False)

        writecsv2 = csv.writer(logfile, quoting=csv.QUOTE_ALL)
        linePattern = re.compile(r"\((\d+.\d+)\)\s+[^\s]+\s+([0-9A-F#]{3}|[0-9A-F#]{8})#([0-9A-F]+)")
        for row in tqdm(inputfile,desc= "Lines", total = numlines,unit = " Lines"):
            try:
                tokens = linePattern.search(row).groups()
                timestamp = float(tokens[0])
                arbitration_id = int(tokens[1],16)
                data = bytearray.fromhex(tokens[2])
                if validate_decode() == True:
                    signals_bool = 1
                    if starttime == 0:
                        starttime = timestamp
                        lastwritetime = timestamp
                        timestamp = 0
                    else:
                        timestamp = (timestamp - starttime)
                    decoded_msg = db.decode_message(arbitration_id, data, decode_choices=False) 
                    for (key, value) in decoded_msg.items():
                        if key in signalList:
                            indexval = signalList.index(key)
                            if signalMin[indexval] == None or value > signalMin[indexval] and signalMax[indexval] == None or value < signalMax[indexval]:
                                if dps_list[indexval] != None:
                                    try:
                                        value = round(float(value),dps_list[indexval])
                                        try:
                                            if int(value) == float(value):
                                                value = int(value)
                                        except:
                                            pass
                                    except:
                                        pass
                                values_list[indexval].append(value)
                if (timestamp - lastwritetime >= (1/frequency)) and (signals_bool == 1) :
                    lastwritetime = timestamp
                    for i, items in enumerate(values_list):
                        if len(values_list[i]) > 0:
                            try:
                                value = sum(values_list[i])/len(values_list[i])
                            except:
                                value = values_list[i][-1]
                            if dps_list[i] != 'None':
                                try:
                                    value = round(float(value),dps_list[i])
                                    try:
                                        if int(value) == float(value):
                                            value = int(value)
                                    except:
                                        pass
                                except:
                                    pass
                            aggregated_values_list[i] = value
                    aggregated_values_list[0] = str("%0.3f" %(lastwritetime-starttime))
                    writecsv.writerow(aggregated_values_list)
                    outputlinecount += 1
                    signals_bool = 0
            
                    for i,items in enumerate(values_list):
                        if aggregated_values_list[i] != "" and signalactive_list[i] == False:
                            signalactive_list[i] = True
                        values_list[i] = []
                        aggregated_values_list[i] = ''
            except:
                print("invalidated line observed: '%s'"% (row[:-1]))
    logfile.close()
inputfile.close()

with open(tempfile, "r") as inputfile:
    with open(outputfile, "w", newline='') as logfile:
        reader = csv.reader(inputfile, delimiter = ',', quotechar = '"')
        writer = csv.writer(logfile,quoting=csv.QUOTE_ALL, delimiter=",")
        for row in tqdm(reader,desc = "Compressing", total = outputlinecount, unit = "Rows"):
            result = list(compress(row, signalactive_list))
            writer.writerow(result)
    logfile.close()
inputfile.close()
os.remove(tempfile)
