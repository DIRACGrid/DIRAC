#!/usr/bin/env python
""" This script instantiate a DFC client against a given service,
    and hammers it with read request (listDirectory) for a given time.
    It produces two files : time.txt and clock.txt which contain time measurement,
    using time.time and time.clock (see respective doc)
    It assumes that the DB has been filled with the scripts in generateDB

    Tunable parameters:
      * maxDuration : time it will run. Cannot be too long, otherwise job
                      is killed because staled
      * port: list of ports on which we can find a service (assumes all the service running on one machine)
      * hostname: name of the host hosting the service
      * readDepth: depth of the path when reading


The depths are to be put in relation with the depths you used to generate the db
"""
import DIRAC

DIRAC.initialize()  # Initialize configuration

import os
import random
import time

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

port = random.choice([9196, 9197, 9198, 9199])
hostname = "yourmachine.somewhere.something"
servAddress = f"dips://{hostname}:{port}/DataManagement/FileCatalog"

maxDuration = 1800  # 30mn

fc = FileCatalogClient(servAddress)

# lfc size = 9, huge db small req = 12, huge db big = 6
readDepth = 12

f = open("time.txt", "w")
f2 = open("clock.txt", "w")
f.write("QueryStart\tQueryEnd\tQueryTime\textra(port %s)\n" % port)
f2.write("QueryStart\tQueryEnd\tQueryClock\textra(port %s)\n" % port)

start = time.time()

done = False

while not done:
    # Between 0 and 3 because in generate we have 4 subdirs per dir. Adapt :-)
    rndTab = [random.randint(0, 3) for i in range(readDepth)]
    dirPath = "/" + "/".join(map(str, rndTab))
    before = time.time()
    beforeC = time.clock()
    res = fc.listDirectory(dirPath)
    afterC = time.clock()
    after = time.time()
    queryTime = after - before
    queryTimeC = afterC - beforeC
    if not res["OK"]:
        extra = res["Message"]
    else:
        out = res["Value"]["Successful"][dirPath]
        extra = "{} {} {}".format(dirPath, len(out["Files"]), len(out["SubDirs"]))

    f.write(f"{before}\t{after}\t{queryTime}\t{extra}\n")
    f.flush()
    os.fsync(f)
    f2.write(f"{beforeC}\t{afterC}\t{queryTimeC}\t{extra}\n")
    f2.flush()
    os.fsync(f2)
    if time.time() - start > maxDuration:
        done = True

f.close()
f2.close()
