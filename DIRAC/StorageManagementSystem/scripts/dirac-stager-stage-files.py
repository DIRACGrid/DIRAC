#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-stage-files
# Author :  Daniela Remenska
########################################################################
"""
- submit staging requests for a particular Storage Element! Default DIRAC JobID will be =0. 
  (not visible in the Job monitoring list though)

"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s <LFN> <SE>' % Script.scriptName,
                                     'Arguments:',
                                     '  <LFN>: LFN to Stage (or local file with list of LFNs)',
                                     '  <SE>:  Name of Storage Element' ] ) )

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

if len( args ) < 2:
  Script.showHelp()

seName = args[1]
fileName = args[0]

import os
from DIRAC import exit as DIRACExit, gLogger
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

stageLfns = {}

if os.path.exists( fileName ):
  try:
    lfnFile = open( fileName )
    lfns = [ k.strip() for k in lfnFile.readlines() ]
    lfnFile.close()
  except Exception:
    gLogger.exception( 'Can not open file', fileName )
    DIRACExit( -1 )
else:
  lfns = args[:len(args)-1]

stageLfns[seName] = lfns
stagerClient = StorageManagerClient()

res = stagerClient.setRequest( stageLfns, 'WorkloadManagement',
                                      'updateJobFromStager@WorkloadManagement/JobStateUpdate',
                                      0 ) # fake JobID = 0
if not res['OK']:
  gLogger.error( res['Message'] )
  DIRACExit( -1 )
else:
  print "Stage request submitted for LFNs:\n %s" %lfns
  print "SE= %s" %seName
  print "You can check their status and progress with dirac-stager-monitor-file <LFN> <SE>"

'''Example1:
dirac-stager-stage-files.py filesToStage.txt GRIDKA-RDST 
Stage request submitted for LFNs:
 ['/lhcb/LHCb/Collision12/FULL.DST/00020846/0002/00020846_00023458_1.full.dst', '/lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032669_1.full.dst', '/lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032666_1.full.dst']
SE= GRIDKA-RDST
You can check their status and progress with dirac-stager-monitor-file <LFN> <SE>

Example2:
dirac-stager-stage-files.py /lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032641_1.full.dst GRIDKA-RDST 
Stage request submitted for LFNs:
 ['/lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032641_1.full.dst']
SE= GRIDKA-RDST
You can check their status and progress with dirac-stager-monitor-file <LFN> <SE>
'''

DIRACExit()
