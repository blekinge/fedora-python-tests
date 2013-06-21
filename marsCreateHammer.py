#!/usr/bin/python

import urllib
import time
import os, shutil
import sys
import scipy
from numpy import *
import matplotlib.pyplot as plt
import threading
import requests


def makeHistogram(data, minval, maxval, datadir):
    hist, bins = histogram(data[0:len(data), 2], range(int(minval), int(maxval)), normed=True)
    center = (bins[:-1]+bins[1:])/2
    plt.bar(center, hist)
    plt.savefig(datadir+"/histogram.png", format='png')
    plt.show()
    

def writeOutputs(data, metrics, dataDir) :
    #Write data
    dataFile = open(dataDir + "/data",'w')
    for n in range(0, len(data)) :
        str = "%13d\t %13d\t %5d\n" % (data[n,0], data[n,1], data[n,2])
        dataFile.write(str)
    dataFile.flush()
    dataFile.close()
        
    #Write statistics
    metricsFile = open(dataDir + "/metrics", 'w')
    for key in metrics.keys() :
        str = "%10s\t%10f\n" % (key, metrics[key])
        metricsFile.write(str)
    metricsFile.flush()
    metricsFile.close()
    
# Build the url for the create call.
def buildUrl(baseurl):
    data = {'label' : 'foo', 'logMessage' : 'my message', 'namespace' : 'default', 'ownerID' : 'me', 'ignoreMime' : 'true', 'ignoreContent':'true'}
    url = baseurl
    urlParams = None
    for key in data.keys():
        if(urlParams == None):
            urlParams = "?" + key + "=" + urllib.quote(data[key])
        else :
            urlParams += "&" + key + "=" + urllib.quote(data[key])

    url += urlParams
    return url


def makeRequest():
    contentType = {'Content-Type':'text/xml'}
    tcall = int(round(time.time() * 1000))
    requests.post(createUrl, auth=(fedoraUser, fedoraPass), headers=contentType)
    treturn = int(round(time.time() * 1000))
    return tcall, treturn

class hammerThread(threading.Thread):
    def __init__(self, timingData, offset, iterations):
        threading.Thread.__init__(self)
        self.timingData = timingData
        self.offset = offset
        self.iterations = iterations

    def run(self):
        for i in range(0, self.iterations):
            tcall, treturn = makeRequest()
            timingData[self.offset+i,0] = tcall
            timingData[self.offset+i,1] = treturn
            timingData[self.offset+i,2] = treturn-tcall
#        print "Thread with offset: " + str(self.offset) + " has finished"


# Begin 'main'

myDir = "";
if(sys.argv[1] != None) :
    myDir = os.getcwd() + "/" + sys.argv[1]
    if(not os.path.isdir(myDir)) :
        os.mkdir(myDir)
    else : 
        print "data dir exits..."
        sys.exit(1)
else : 
    print "please specify data dir"
    sys.exit(1)

fedoraNewUrl = "http://mars:8080/fedora/objects/new/"
fedoraUser = "fedoraAdmin"
fedoraPass = "fedoraAdminPass"

createUrl = buildUrl(fedoraNewUrl)

#The number of threads to run
numThreads = 10
#The number of datapoints per thread
dataPoints = 10000

threads = []
timingData = zeros((dataPoints*numThreads, 3))
metrics = {}

makeRequest()

for i in range(0, numThreads):
    offset = i * dataPoints
    threads.append(hammerThread(timingData, offset, dataPoints))

tstart = int(round(time.time() * 1000))
for t in threads: 
    t.start()

# Wait for threads to finish
for t in threads: 
    t.join()

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








