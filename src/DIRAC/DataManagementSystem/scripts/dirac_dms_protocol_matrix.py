#!/usr/bin/env python
"""
Generate a matrix of protocols used between SEs for FTS transfers.
The output is a CSV file containing a matrix source/destination.
The value of each cell is of the form "proto1/proto2 ([protoA,protoB,...])"
proto1/proto2 are the protocols that would really be given to FTS for source and dest urls
protoA,protoB,etc are the sorted list of protocols that would be attempted for thrid party copy by DIRAC

By default, all the SEs are taken into account, but the matrix is factorized by using baseSEs.

Suppose you have the following in your CS::

  StorageElementBases{
    IN2P3-Disk
  }
  StorageElements{
    IN2P3-DST{
      BaseSE = IN2P3-Disk
    }
    IN2P3-User{
      BaseSE = IN2P3-Disk
    }
    AnotherDisk{
    }
  }

You can have the following combinations::

  DIRAC-PROD>dirac-dms-protocol-matrix
  Using sources: IN2P3-Disk, AnotherDisk
  Using target: IN2P3-Disk, AnotherDisk

  DIRAC-PROD>dirac-dms-protocol-matrix --FromSE=IN2P3-User
  Using sources: IN2P3-User
  Using target: IN2P3-Disk, AnotherDisk

  DIRAC-PROD>dirac-dms-protocol-matrix --FromSE=IN2P3-User --Bidirection
  Using sources: IN2P3-User
  Using target: IN2P3-User
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import csv
from collections import defaultdict

from DIRAC.Core.Utilities.DIRACScript import DIRACScript

@DIRACScript()
def main():
  from DIRAC.Core.Base import Script
  Script.registerSwitch('', 'FromSE=', 'SE1[,SE2,...]')
  Script.registerSwitch('', 'TargetSE=', 'SE1[,SE2,...]')
  Script.registerSwitch('', 'OutputFile=', 'CSV output file (default /tmp/protocol-matrix.csv)')
  Script.registerSwitch('', 'Bidirection', 'If FromSE or TargetSE are specified, make a square matrix ')
  Script.registerSwitch('', 'FTSOnly', 'Only display the protocols sent to FTS')
  Script.registerSwitch('', 'ExcludeSE=', 'SEs to not take into account for the matrix')

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()
  from DIRAC import gConfig, gLogger, S_ERROR
  from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
  from DIRAC.DataManagementSystem.private.FTS3Utilities import getFTS3Plugin
  from DIRAC.Resources.Storage.StorageElement import StorageElement
  from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

  fromSE = []
  targetSE = []
  excludeSE = []
  outputFile = '/tmp/protocol-matrix.csv'
  bidirection = False
  ftsOnly = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'FromSE':
      fromSE = switch[1].split(',')
    elif switch[0] == 'TargetSE':
      targetSE = switch[1].split(',')
    elif switch[0] == 'ExcludeSE':
      excludeSE = switch[1].split(',')
    elif switch[0] == 'OutputFile':
      outputFile = switch[1]
    elif switch[0] == 'Bidirection':
      bidirection = True
    elif switch[0] == 'FTSOnly':
      ftsOnly = True

  fts3Plugin = getFTS3Plugin()
  thirdPartyProtocols = DMSHelpers().getThirdPartyProtocols()

  # List all the BaseSE
  seBases = gConfig.getSections('/Resources/StorageElementBases')['Value']
  # construct a dict { baseSE : <inherited storages>}
  seForSeBases = {}

  allSEs = gConfig.getSections('/Resources/StorageElements/')['Value']

  # Remove the SEs that we want to exclude
  allSEs = set(allSEs) - set(excludeSE)

  # We go through all the SEs and fill in the seForSEBases dict.
  # Basically, at the end of the loop, the dict will contain
  # for each baseSE an entry corresponding to one real storage (the first one)
  # and itself for each real non inherited SE
  for se in allSEs:
    baseSE = gConfig.getOption('/Resources/StorageElements/%s/BaseSE' % se).get('Value')
    if baseSE:
      if baseSE not in seForSeBases:
        seForSeBases[baseSE] = se
    else:
      # If no baseSE, we put self
      seForSeBases[se] = se

  # Now let's take into account what source and destination we want.

  # If the user did not specify source or dest, generate everything
  if not fromSE and not targetSE:
    fromSE = list(seForSeBases)
    targetSE = list(seForSeBases)
  else:  # he specified at least source of dest

    # if bidirection, source and target should be the same
    if bidirection:
      if not fromSE and targetSE:  # we gave target, but no source
        fromSE = targetSE
      elif fromSE and not targetSE:  # we gave source but no target
        targetSE = fromSE
      elif fromSE and targetSE:  # we gave both
        fromSE = targetSE = list(set(fromSE + targetSE))

    else:  # no bidirection
      # he specified a targetSE
      if not fromSE:
        fromSE = list(seForSeBases)
      elif not targetSE:
        targetSE = list(seForSeBases)

  fromSE = sorted(fromSE)
  targetSE = sorted(targetSE)

  gLogger.notice("Using sources: %s" % ','.join(fromSE))
  gLogger.notice("Using target: %s" % ','.join(targetSE))

  # Now we construct the SE object for each SE that we want to appear
  ses = {}
  for se in set(fromSE + targetSE):
    ses[se] = StorageElement(seForSeBases.get(se, se))

  ret = getVOfromProxyGroup()
  if not ret['OK'] or not ret.get('Value', ''):
    gLogger.error('Aborting, Bad Proxy:', ret.get('Message', 'Proxy does not belong to a VO!'))
    exit(1)
  vo = ret['Value']
  gLogger.notice('Using the Virtual Organization:', vo)
  # dummy LFN, still has to follow lfn convention
  lfn = '/%s/toto.xml' % vo

  # Create a matrix of protocol src/dest

  tpMatrix = defaultdict(dict)

  # For each source and destination, generate the url pair, and the compatible third party protocols
  for src, dst in ((x, y) for x in fromSE for y in targetSE):
    try:
      fts3TpcProto = fts3Plugin.selectTPCProtocols(sourceSEName=src, destSEName=dst)
      res = ses[dst].generateTransferURLsBetweenSEs(lfn, ses[src], fts3TpcProto)
    except ValueError as e:
      res = S_ERROR(str(e))
    if not res['OK']:
      surls = 'Error'
      gLogger.notice("Could not generate transfer URLS", "src:%s, dst:%s, error:%s" % (src, dst, res['Message']))
    else:
      # We only keep the protocol part of the url
      surls = '/'.join(res['Value']['Protocols'])

    # Add also the third party protocols
    proto = ','.join(ses[dst].negociateProtocolWithOtherSE(ses[src], thirdPartyProtocols)['Value'])
    if ftsOnly:
      tpMatrix[src][dst] = '%s' % surls
    else:
      tpMatrix[src][dst] = '%s (%s)' % (surls, proto)
    gLogger.verbose("%s -> %s: %s" % (src, dst, surls))
    gLogger.verbose("%s -> %s: %s" % (src, dst, proto))

  # Write the matrix in the file
  with open(outputFile, 'wb') as csvfile:
    csvWriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    csvWriter.writerow(['src/dst'] + targetSE)

    for src in fromSE:
      srcRow = [src]
      for dst in targetSE:
        srcRow.append(tpMatrix[src].get(dst, 'NA'))
      csvWriter.writerow(srcRow)
  gLogger.notice('Wrote Matrix to', outputFile)


if __name__ == "__main__":
  main()
