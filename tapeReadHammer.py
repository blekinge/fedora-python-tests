#!/usr/bin/python

import time
import os, shutil
import sys
import scipy
from numpy import *
import matplotlib.pyplot as plt
import random
import threading
import requests

def makeHistogram(data, minval, maxval, datadir):
    hist, bins = histogram(data[0:len(data), 2], range(int(minval), int(maxval)), normed=True)
    center = (bins[:-1]+bins[1:])/2
    plt.bar(center, hist)
    plt.savefig(datadir+"/histogram.png", format='png')
    plt.show()
    

def writeOutputs(data, metrics, dataDir):
    #Write data
    dataFile = open(dataDir + "/data",'w')
    for n in range(0, len(data)):
        str = "%13d\t %13d\t %5d\n" % (data[n,0], data[n,1], data[n,2])
        dataFile.write(str)
    dataFile.flush()
    dataFile.close()
        
    #Write statistics
    metricsFile = open(dataDir + "/metrics", 'w')
    for key in metrics.keys():
        str = "%10s\t%10f\n" % (key, metrics[key])
        metricsFile.write(str)
    metricsFile.flush()
    metricsFile.close()


def makeRequest(readurl, pool):
    id = idPool[random.choice(pool)].split("/")[1].split("#")[0]
    url = readurl.replace("{obj}", str(id))
    tcall = int(round(time.time() * 1000))
    data = None;
    try:
        data = requests.get(url, auth=(fedoraUser, fedoraPass))
    except requests.exceptions.ConnectionError as e:
        x = "I/O error: {0}".format(e)
        print "Failure '"+x+"' on ID: " + str(id)
    treturn = int(round(time.time() * 1000))
    if(data != None):
        if not id in data.text.split('\n')[1]:
            print "Got bad object from service. Expected ID: '" + str(id) + "' got '" + data.text.split('\n')[1] + "'"
    return tcall, treturn

def loadIDs(IDsFile):
    print "loading ids from file"
    ids = []
    f = open(IDsFile, 'r')
    for line in f:
        ids.append(line)
        
    return ids


class hammerThread(threading.Thread):

    def __init__(self, timingData, offset, iterations):
        threading.Thread.__init__(self)
        self.timingData = timingData
        self.offset = offset
        self.iterations = iterations
        self.progress=0
        self.kill_received = False

    def run(self):
        for i in range(0, self.iterations):
            if self.kill_received:
                break
            tcall, treturn = makeRequest(readurl, pool)
            timingData[self.offset+i,0] = tcall
            timingData[self.offset+i,1] = treturn
            timingData[self.offset+i,2] = treturn-tcall
            self.progress+=1
#        print "Thread with offset: " + str(self.offset) + " has finished"

# Begin 'main'

myDir = "";
if(sys.argv[1] != None) :
    myDir = os.getcwd() + "/" + sys.argv[1];
    if(not os.path.isdir(myDir)) :
        os.mkdir(myDir);
    else : 
        print "data dir exits..."
        sys.exit(1);
else : 
    print "please specify data dir"
    sys.exit(1);

baseurl = "http://mars:7880/fedora/objects/"
readurl = baseurl + "{obj}/objectXML"
fedoraUser = "fedoraAdmin"
fedoraPass = "fedoraAdminPass"
idsFile = os.getcwd() + "/ids"

idPool = loadIDs(idsFile)

# The number of threads to run
numThreads = 10
# The number of datapoints per thread
dataPoints = 10000


pool = range(0,len(idPool))
threads = []
timingData = zeros((dataPoints*numThreads, 3))
metrics = {}

makeRequest(readurl, pool)

print "Setup and start threads"
for i in range(0, numThreads):
    offset = i * dataPoints
    threads.append(hammerThread(timingData, offset, dataPoints))

tstart = int(round(time.time() * 1000))
for t in threads: 
    t.start()

print "Wait for threads to finish"

while len(threads) > 0:
        try:
            # Join all threads using a timeout so it doesn't block
            # Filter out threads which have been joined or are None
            threads2 = []
            for t in threads:
                    if (t is not None and t.isAlive()):
                            t.join(10)
                            if t.isAlive():
                                threads2.append(t)
                                print t.name+":"+str(t.progress *100 / t.iterations)+"%"
                    
            threads = threads2
        except KeyboardInterrupt:
            print "Ctrl-c received! Sending kill to threads..."
            for t in threads:
                t.kill_received = True


tstop = int(round(time.time() * 1000))

metrics['min'] = amin(timingData[:, 2])
metrics['max'] = amax(timingData[:, 2])
metrics['avg'] = average(timingData[:, 2])
metrics['median'] = median(timingData[:, 2])
metrics['std'] = std(timingData[:, 2])
metrics['var'] = var(timingData[:, 2])
metrics['totalTime'] = tstop - tstart
metrics['datapoints'] = dataPoints*numThreads

print metrics

writeOutputs(timingData, metrics, myDir)
makeHistogram(timingData, metrics['min'], metrics['max'], myDir)








