#! /usr/bin/env python 
from DIRAC.Core.Base                                           import Script
Script.parseCommandLine()

import DIRAC
from DIRAC                                                     import gLogger,S_OK,S_ERROR
from DIRAC.Core.Utilities.List                                 import sortList
from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
integrityClient = DataIntegrityClient()

def resolveTransforamtionProblematics(transID):
  gLogger.info("Obtaining problematic files for transformation %d" % transID)
  res = integrityClient.getTransformationProblematics(transID)
  if not res['OK']:
    gLogger.error("Failed to get transformation problematic files",res['Message'])
    return S_ERROR()
  problematicFiles = res['Value']
  if not problematicFiles:
    gLogger.info("No problematic files found for transformation")
    return S_OK()
  for lfn in sortList(problematicFiles.keys()):
    prognosis = problematicFiles[lfn]['Prognosis']
    problematicDict = problematicFiles[lfn]
    execString = "res = integrityClient.resolve%s(problematicDict)" % prognosis
    try:
      exec(execString)
    except AttributeError:
      gLogger.error("Resolution method for %s not available" % prognosis)
  gLogger.info("Problematic files resolved for transformation %d" % transID) 
  return S_OK() 

transIDs = [int(x) for x in Script.getPositionalArgs()]
if not transIDs:
  gLogger.info("Please supply transformationIDs as arguments")
  DIRAC.exit(0)
for transID in transIDs:
  resolveTransforamtionProblematics(transID)
