#!/usr/bin/env python

""" This script instantiate a DFC client against a given service,
    and hammers it with mixed request (read/write/delete) for a given time.
    It produces two files : time.txt and clock.txt which contain time measurement,
    using time.time and time.clock (see respective doc)
    It assumes that the DB has been filled with the scripts in generateDB

    Tunable parameters:
      * maxDuration : time it will run. Cannot be too long, otherwise job
                      is killed because staled
      * port: list of ports on which we can find a service (assumes all the service running on one machine)
      * hostname: name of the host hosting the service
      * storageElements: list of storage element names
      * writeDepth: depth of the path when writing
      * maxInsert: max number of files inserted at the same time

The depths are to be put in relation with the depths you used to generate the db
"""
import DIRAC

DIRAC.initialize()  # Initialize configuration

import os
import random
import time
import string
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

port = random.choice([9196, 9197, 9198, 9199])
hostname = "yourmachine.somewhere.something"
servAddress = f"dips://{hostname}:{port}/DataManagement/FileCatalog"

maxDuration = 1800  # 30mn
maxInsert = 10
fc = FileCatalogClient(servAddress)

storageElements = ["se0", "se1", "se2", "se3", "se4", "se5", "se6", "se7", "se8", "se9"]

writeDepth = 13

fl = open("time.txt", "w")
fl2 = open("clock.txt", "w")
fl.write(f"QueryStart\tQueryEnd\tQueryTime\textra(port {port})\n")
fl2.write(f"QueryStart\tQueryEnd\tQueryClock\textra(port {port})\n")

start = time.time()

done = False

while not done:
    # Between 0 and 3 because in generate we have 4 subdirs per dir. Adapt :-)
    rndTab = [random.randint(0, 3) for i in range(writeDepth)]
    rndLetters = [random.choice(string.ascii_letters) for i in range(3)]
    rndTab += rndLetters
    dirPath = "/" + "/".join(map(str, rndTab))
    nbOfFiles = random.randint(1, maxInsert)
    lfnDict = {}
    for f in range(nbOfFiles):
        filename = f"{f}.txt"
        lfn = f"{dirPath}/{filename}"
        size = random.randint(1, 1000)
        se = random.choice(storageElements)
        guid = f"{''.join(map(str, rndTab))}{filename}"
        checksum = guid
        lfnDict[lfn] = {"PFN": lfn, "SE": se, "Size": size, "GUID": guid, "Checksum": checksum}

    beforeI = time.time()
    beforeCI = time.clock()
    res = fc.addFile(lfnDict)
    afterCI = time.clock()
    afterI = time.time()
    queryInsertTime = afterI - beforeI
    queryInsertTimeC = afterCI - beforeCI
    extra = "insert "
    if not res["OK"]:
        extra += res["Message"]
    else:
        extra += "{} {} {}".format(
            len(lfnDict),
            len(res["Value"].get("Successful", [])),
            len(res["Value"].get("Failed", [])),
        )
        pass

    fl.write(f"{beforeI}\t{afterI}\t{queryInsertTime}\t{extra}\n")
    fl.flush()
    os.fsync(fl)
    fl2.write(f"{beforeCI}\t{afterCI}\t{queryInsertTimeC}\t{extra}\n")
    fl2.flush()
    os.fsync(fl2)

    beforeR = time.time()
    beforeCR = time.clock()
    res = fc.removeFile(lfnDict)
    afterCR = time.clock()
    afterR = time.time()
    queryRemoveTimeC = afterCR - beforeCR
    queryRemoveTime = afterR - beforeR
    extra = "remove "
    if not res["OK"]:
        extra += res["Message"]
    else:
        extra += "{} {} {}".format(
            nbOfFiles, len(res["Value"].get("Successful", [])), len(res["Value"].get("Failed", []))
        )
        pass

    fl.write(f"{beforeR}\t{afterR}\t{queryRemoveTime}\t{extra}\n")
    fl.flush()
    os.fsync(fl)
    fl2.write(f"{beforeCR}\t{afterCR}\t{queryRemoveTimeC}\t{extra}\n")
    fl2.flush()
    os.fsync(fl2)
    if time.time() - start > maxDuration:
        done = True

fl.close()
fl2.close()
