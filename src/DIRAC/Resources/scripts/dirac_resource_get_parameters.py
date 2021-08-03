#!/usr/bin/env python
"""
Get parameters assigned to the CE
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import json
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script

ceName = ''
Queue = ''
Site = ''


@Script()
def main():
  global ceName
  global Queue
  global Site

  from DIRAC import gLogger, exit as DIRACExit
  from DIRAC.ConfigurationSystem.Client.Helpers import Resources
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getVMTypes

  def setCEName(args):
    global ceName
    ceName = args

  def setSite(args):
    global Site
    Site = args

  def setQueue(args):
    global Queue
    Queue = args

  Script.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)", setCEName)
  Script.registerSwitch("S:", "Site=", "Site Name (Mandatory)", setSite)
  Script.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", setQueue)

  Script.parseCommandLine(ignoreErrors=True)

  result = Resources.getQueue(Site, ceName, Queue)

  if not result['OK']:
    # Normal DIRAC queue search failed, check for matching VM images
    vmres = getVMTypes(Site, ceName, Queue)
    if vmres['OK']:
      # VM type found, return the details for that
      # Shuffle results around to match original queue spec: Does this actually do useful stuff now?
      queueDict = result['Value'][Site][ceName].pop('VMTypes')[Queue]
      ceDict = result['Value'][Site][ceName]
      ceDict.update(queueDict)
      gLogger.notice(json.dumps(result['Value']))
      return
    # Queue & VM not found, return original queue failure message
    gLogger.error("Could not retrieve resource parameters", ": " + result['Message'])
    DIRACExit(1)
  gLogger.notice(json.dumps(result['Value']))


if __name__ == "__main__":
  main()
