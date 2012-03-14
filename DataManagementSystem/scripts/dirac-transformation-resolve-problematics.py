#! /usr/bin/env python 
########################################################################
# $HeadURL:  $
########################################################################
__RCSID__   = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Resolve problematic files for the specified transformations

Usage:
   %s [options] TransID [TransID] 
""" % Script.scriptName)

Script.parseCommandLine()

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import sortList
from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
integrityClient = DataIntegrityClient()

def resolveTransforamtionProblematics( transID ):
  gLogger.notice("Obtaining problematic files for transformation %d" % transID)
  res = integrityClient.getTransformationProblematics(transID)
  if not res['OK']:
    gLogger.error("Failed to get transformation problematic files", res['Message'])
    return S_ERROR()
  problematicFiles = res['Value']
  if not problematicFiles:
    gLogger.notice("No problematic files found for transformation")
    return S_OK()
  for lfn in sortList(problematicFiles.keys()):
    prognosis = problematicFiles[lfn]['Prognosis']
    problematicDict = problematicFiles[lfn]
    gLogger.notice("Prognosis is %s" % prognosis )
    if not hasattr( integrityClient, methodToCall ):
      gLogger.notice( "DataIntegrityClient hasn't got '%s' member" % methodToCall )
      continue
    fcn = getattr( integrityClient, methodToCall )
    if not callable( fcn ):
      gLogger.notice( "DataIntegrityClient member '%s' isn't a method" % methodToCall )
      continue
    ## results not checked??? Where is The Food?
    res = fcn( problematicDict )
  gLogger.notice("Problematic files resolved for transformation %d" % transID) 
  return S_OK() 

transIDs = [ int(x) for x in Script.getPositionalArgs() ]

if not transIDs:
  gLogger.notice("Please supply transformationIDs as arguments")
  DIRAC.exit(0)
for transID in transIDs:
  resolveTransforamtionProblematics(transID)
