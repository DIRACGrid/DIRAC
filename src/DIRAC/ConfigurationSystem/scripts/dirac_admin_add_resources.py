#!/usr/bin/env python
########################################################################
# File :   dirac-admin-add-resources
# Author : Andrei Tsaregorodtsev
########################################################################
"""
Add resources from the BDII database for a given VO
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import signal
import shlex

import six

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC import gLogger, exit as DIRACExit
from DIRAC.ConfigurationSystem.Client.Utilities import getGridCEs, getSiteUpdates
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues, getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption


def processScriptSwitches():

  global vo, dry, doCEs, hostURL, glue2

  Script.registerSwitch("V:", "vo=", "Virtual Organization")
  Script.registerSwitch("D", "dry", "Dry run")
  Script.registerSwitch("C", "ce", "Process Computing Elements")
  Script.registerSwitch("H:", "host=", "use this url for information querying")
  Script.registerSwitch("G", "glue2", "DEPRECATED: query GLUE2 information schema")
  Script.registerSwitch("g", "glue1", "query GLUE1 information schema")
  Script.parseCommandLine(ignoreErrors=True)

  vo = ''
  dry = False
  doCEs = False
  hostURL = None
  glue2 = True
  for sw in Script.getUnprocessedSwitches():
    if sw[0] in ("V", "vo"):
      vo = sw[1]
    if sw[0] in ("D", "dry"):
      dry = True
    if sw[0] in ("C", "ce"):
      doCEs = True
    if sw[0] in ("H", "host"):
      hostURL = sw[1]
    if sw[0] in ("G", "glue2"):
      gLogger.notice(" The '-G' flag is deprecated, Glue2 is the default now")
    if sw[0] in ("g", "glue1"):
      glue2 = False


ceBdiiDict = None


def checkUnusedCEs():

  global vo, dry, ceBdiiDict, hostURL, glue2

  gLogger.notice('looking for new computing resources in the BDII database...')

  res = getQueues(community=vo)
  if not res['OK']:
    gLogger.error('ERROR: failed to get CEs from CS', res['Message'])
    DIRACExit(-1)

  knownCEs = set()
  for _site, ces in res['Value'].items():
    knownCEs.update(ces)

  result = getGridCEs(vo, ceBlackList=knownCEs, hostURL=hostURL, glue2=glue2)
  if not result['OK']:
    gLogger.error('ERROR: failed to get CEs from BDII', result['Message'])
    DIRACExit(-1)
  ceBdiiDict = result['BdiiInfo']

  unknownCEs = result['UnknownCEs']
  if unknownCEs:
    gLogger.notice('There is no (longer) information about the following CEs for the %s VO:' % vo)
    gLogger.notice('\n'.join(sorted(unknownCEs)))

  siteDict = result['Value']
  if siteDict:
    gLogger.notice('New resources available:')
    for site in siteDict:
      diracSite = 'Unknown'
      result = getDIRACSiteName(site)
      if result['OK']:
        diracSite = ','.join(result['Value'])
      if siteDict[site]:
        gLogger.notice("  %s, DIRAC site %s" % (site, diracSite))
        for ce in siteDict[site]:
          gLogger.notice(' ' * 4 + ce)
          gLogger.notice('      %s, %s' % (siteDict[site][ce]['CEType'], '%s_%s_%s' % siteDict[site][ce]['System']))
  else:
    gLogger.notice('No new resources available, exiting')
    return

  inp = six.moves.input("\nDo you want to add sites ? [default=yes] [yes|no]: ")
  inp = inp.strip()
  if not inp and inp.lower().startswith('n'):
    return

  gLogger.notice('\nAdding new sites/CEs interactively\n')

  sitesAdded = []

  for site in siteDict:
    # Get the country code:
    country = ''
    for ce in siteDict[site]:
      country = ce.strip().split('.')[-1].lower()
      if len(country) == 2:
        break
      if country == 'gov':
        country = 'us'
        break
    if not country or len(country) != 2:
      country = 'xx'
    result = getDIRACSiteName(site)
    if not result['OK']:
      gLogger.notice('\nThe site %s is not yet in the CS, give it a name' % site)
      diracSite = six.moves.input('[help|skip|<domain>.<name>.%s]: ' % country)
      if diracSite.lower() == "skip":
        continue
      if diracSite.lower() == "help":
        gLogger.notice('%s site details:' % site)
        for k, v in ceBdiiDict[site].items():
          if k != "CEs":
            gLogger.notice('%s\t%s' % (k, v))
        gLogger.notice('\nEnter DIRAC site name in the form <domain>.<name>.%s\n' % country)
        diracSite = six.moves.input('[<domain>.<name>.%s]: ' % country)
      try:
        _, _, _ = diracSite.split('.')
      except ValueError:
        gLogger.error('ERROR: DIRAC site name does not follow convention: %s' % diracSite)
        continue
      diracSites = [diracSite]
    else:
      diracSites = result['Value']

    if len(diracSites) > 1:
      gLogger.notice('Attention! GOC site %s corresponds to more than one DIRAC sites:' % site)
      gLogger.notice(str(diracSites))
      gLogger.notice('Please, pay attention which DIRAC site the new CEs will join\n')

    newCEs = {}
    addedCEs = []
    for ce in siteDict[site]:
      ceType = siteDict[site][ce]['CEType']
      for diracSite in diracSites:
        if ce in addedCEs:
          continue
        yn = six.moves.input("Add CE %s of type %s to %s? [default yes] [yes|no]: " % (ce, ceType, diracSite))
        if yn == '' or yn.lower().startswith('y'):
          newCEs.setdefault(diracSite, [])
          newCEs[diracSite].append(ce)
          addedCEs.append(ce)

    for diracSite in diracSites:
      if diracSite in newCEs:
        cmd = "dirac-admin-add-site %s %s %s" % (diracSite, site, ' '.join(newCEs[diracSite]))
        gLogger.notice("\nNew site/CEs will be added with command:\n%s" % cmd)
        yn = six.moves.input("Add it ? [default yes] [yes|no]: ")
        if not (yn == '' or yn.lower().startswith('y')):
          continue

        if dry:
          gLogger.notice("Command is skipped in the dry run")
        else:
          result = systemCall(0, shlex.split(cmd))
          if not result['OK']:
            gLogger.error('Error while executing dirac-admin-add-site command')
            yn = six.moves.input("Do you want to continue ? [default no] [yes|no]: ")
            if yn == '' or yn.lower().startswith('n'):
              if sitesAdded:
                gLogger.notice('CEs were added at the following sites:')
                for site, diracSite in sitesAdded:
                  gLogger.notice("%s\t%s" % (site, diracSite))
              DIRACExit(0)
          else:
            exitStatus, stdData, errData = result['Value']
            if exitStatus:
              gLogger.error('Error while executing dirac-admin-add-site command\n', '\n'.join([stdData, errData]))
              yn = six.moves.input("Do you want to continue ? [default no] [yes|no]: ")
              if yn == '' or yn.lower().startswith('n'):
                if sitesAdded:
                  gLogger.notice('CEs were added at the following sites:')
                  for site, diracSite in sitesAdded:
                    gLogger.notice("%s\t%s" % (site, diracSite))
                DIRACExit(0)
            else:
              sitesAdded.append((site, diracSite))
              gLogger.notice(stdData)

  if sitesAdded:
    gLogger.notice('CEs were added at the following sites:')
    for site, diracSite in sitesAdded:
      gLogger.notice("%s\t%s" % (site, diracSite))
  else:
    gLogger.notice('No new CEs were added this time')


def updateCS(changeSet):

  global vo, dry, ceBdiiDict

  changeList = sorted(changeSet)
  if dry:
    gLogger.notice('The following needed changes are detected:\n')
  else:
    gLogger.notice('We are about to make the following changes to CS:\n')
  for entry in changeList:
    gLogger.notice("%s/%s %s -> %s" % entry)

  if not dry:
    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
      gLogger.error('Failed to initialize CSAPI object', result['Message'])
      DIRACExit(-1)
    for section, option, value, new_value in changeSet:
      if value == 'Unknown' or not value:
        csAPI.setOption(cfgPath(section, option), new_value)
      else:
        csAPI.modifyValue(cfgPath(section, option), new_value)

    yn = six.moves.input('Do you want to commit changes to CS ? [default yes] [yes|no]: ')
    if yn == '' or yn.lower().startswith('y'):
      result = csAPI.commit()
      if not result['OK']:
        gLogger.error("Error while commit to CS", result['Message'])
      else:
        gLogger.notice("Successfully committed %d changes to CS" % len(changeSet))


def updateSites():

  global vo, dry, ceBdiiDict, glue2

  result = getSiteUpdates(vo, bdiiInfo=ceBdiiDict, glue2=glue2)
  if not result['OK']:
    gLogger.error('Failed to get site updates', result['Message'])
    DIRACExit(-1)
  changeSet = result['Value']

  updateCS(changeSet)


def handler(signum, frame):
  gLogger.notice('\nExit is forced, bye...')
  DIRACExit(-1)


@DIRACScript()
def main():
  signal.signal(signal.SIGTERM, handler)
  signal.signal(signal.SIGINT, handler)

  global vo, dry, doCEs, ceBdiiDict

  processScriptSwitches()

  if not vo:
    gLogger.error('No VO specified')
    DIRACExit(-1)

  vo = getVOOption(vo, 'VOMSName', vo)

  if doCEs:
    yn = six.moves.input('Do you want to check/add new sites to CS ? [default yes] [yes|no]: ')
    yn = yn.strip()
    if yn == '' or yn.lower().startswith('y'):
      checkUnusedCEs()

    yn = six.moves.input('Do you want to update CE details in the CS ? [default yes] [yes|no]: ')
    yn = yn.strip()
    if yn == '' or yn.lower().startswith('y'):
      updateSites()


if __name__ == "__main__":
  vo = ''
  dry = False
  doCEs = False
  ceBdiiDict = None

  main()
