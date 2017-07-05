#!/bin/env python
""" script to obtain release notes from DIRAC PRs
"""

import json
import subprocess
from datetime import datetime, timedelta

from collections import defaultdict
import argparse

try:
  from GitTokens import GITHUBTOKEN
except ImportError:
  raise ImportError("""Failed to import GITHUBTOKEN please point the pythonpath to your GitTokens.py file which contains your "Personal Access Token" for Github

                    I.e.:
                    Filename: GitTokens.py
                    Content:
                    ```
                    GITHUBTOKEN = "e0b83063396fc632646603f113437de9"
                    ```
                    (without the triple quotes)
                    """
                   )


def getCommands( *args ):
  """ create a flat list

  :param *args: list of strings or tuples/lists
  :returns: flattened list of strings
  """
  comList = []
  for arg in args:
    if isinstance( arg, (tuple, list) ):
      comList.extend( getCommands( *arg ) )
    else:
      comList.append(arg)
  return comList


def ghHeaders( ):
  """ return authorization header for github as used by curl

  :returns: tuple to be used in commands list
  """
  return '-H','Authorization: token %s' % GITHUBTOKEN


def curl2Json( *commands, **kwargs ):
  """ return the json object from calling curl with the given commands

  :param *commands: list of strings or tuples/lists, will be passed to `getCommands` to be flattend
  :returns: json object returned from the github or gitlab API
  """
  commands = getCommands( *commands )
  commands.insert( 0, 'curl' )
  commands.insert( 1, '-s' )
  if kwargs.get("checkStatusOnly", False):
    commands.insert( 1, '-I' )
  cleanedCommands = list(commands)
  ## replace the github token with Xs
  if '-H' in cleanedCommands:
    cleanedCommands[commands.index('-H')+1] = cleanedCommands[commands.index('-H')+1].rsplit(" ", 1)[0] + " "+ "X"*len(cleanedCommands[commands.index('-H')+1].rsplit(" ", 1)[1])
  jsonText = subprocess.check_output( commands )
  try:
    jsonList = json.loads( jsonText )
  except ValueError:
    if kwargs.get("checkStatusOnly", False):
      return jsonText
    raise
  return jsonList


def getFullSystemName( name ):
  name = {
    'API': 'Interfaces',
    'AS': 'AccountingSystem',
    'CS': 'ConfigurationSystem',
    'Config': 'ConfigurationSystem',
    'Configuration': 'ConfigurationSystem',
    'DMS': 'DataManagementSystem',
    'DataManagement': 'DataManagementSystem',
    'FS': 'FrameworkSystem',
    'Framework': 'FrameworkSystem',
    'MS': 'MonitoringSystem',
    'Monitoring': 'MonitoringSystem',
    'RMS': 'RequestManagementSystem',
    'RequestManagement': 'RequestManagementSystem',
    'RSS': 'ResourceStatusSystem',
    'ResourceStatus': 'ResourceStatusSystem',
    'SMS': 'StorageManagamentSystem',
    'StorageManagement': 'StorageManagamentSystem',
    'TS': 'TransformationSystem',
    'TMS': 'TransformationSystem',
    'Transformation': 'TransformationSystem',
    'WMS': 'WorkloadManagementSystem',
    'Workload': 'WorkloadManagementSystem',
  }.get( name, name )

  return name

def parseForReleaseNotes( commentBody ):
  """ will look for "BEGINRELEASENOTES / ENDRELEASENOTES" and extend releaseNoteList if there are entries """

  relNotes = ''
  if not all( tag in commentBody for tag in ("BEGINRELEASENOTES", "ENDRELEASENOTES") ):
    return relNotes

  releaseNotes=commentBody.split("BEGINRELEASENOTES")[1].split("ENDRELEASENOTES")[0]
  relNotes = releaseNotes

  return relNotes

def collateReleaseNotes( prs ):
  """put the release notes in the proper order

  FIXME: Tag numbers could be obtained by getting the last tag with a name similar to
  the branch, will print out just the base branch for now.

  """
  releaseNotes = ""
  for baseBranch, pr in prs.iteritems():
    releaseNotes += "[%s]\n\n" % baseBranch
    systemChangesDict = defaultdict( list )
    for prid, content in pr.iteritems():
      notes = content['comment']
      system = ''
      for line in notes.splitlines():
        line = line.strip()
        if line.startswith("*"):
          system = getFullSystemName( line.strip("*:").strip() )
        elif line:
          splitline = line.split(":", 1)
          if splitline[0] == splitline[0].upper() and len(splitline) > 1:
            line = "%s: (#%s) %s" % (splitline[0], prid, splitline[1].strip() )
          systemChangesDict[system].append( line )

    for system, changes in systemChangesDict.iteritems():
      if not system:
        continue
      releaseNotes += "*%s\n\n" % system
      releaseNotes += "\n".join( changes )
      releaseNotes += "\n\n"
    releaseNotes += "\n"

  return releaseNotes

class GithubInterface( object ):
  """ object to make calls to github API

  :param list branches: list of branches to get releases notes for

  """

  def __init__( self, owner='DiracGrid', repo='Dirac'):
    self.owner = owner
    self.repo = repo
    self._options = dict( owner=self.owner, repo=self.repo  )

    self.branches = ['Integration', 'rel-v6r17', 'rel-v6r18']
    self.openPRs = False
    self.startDate = str(datetime.now() - timedelta(days=14))[:10]

  def parseOptions(self):
    """parse the command line options"""
    parser = argparse.ArgumentParser("Dirac Release Notes",
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--branches", action="store", default=self.branches,
                        dest="branches", nargs='+',
                        help="branches to get release notes for")

    parser.add_argument("--date", action="store", default=self.startDate, dest="startDate",
                        help="date after which PRs are checked, default (two weeks ago): %s" % self.startDate)

    parser.add_argument("--openPRs", action="store_true", dest="openPRs", default=self.openPRs,
                        help="get release notes for open (unmerged) PRs, for testing purposes")

    parsed = parser.parse_args()

    self.branches = parsed.branches
    self.startDate = parsed.startDate
    self.openPRs = parsed.openPRs


  def _github( self, action ):
    """ return the url to perform actions on github

    :param str action: command to use in the gitlab API, see documentation there
    :returns: url to be used by curl
    """
    options = dict(self._options)
    options["action"] = action
    ghURL = "https://api.github.com/repos/%(owner)s/%(repo)s/%(action)s" % options
    return ghURL


  def getGithubPRs( self, state="open", mergedOnly=False, perPage=100):
    """ get all PullRequests from github

    :param str state: state of the PRs, open/closed/all, default open
    :param bool merged: if PR has to be merged, only sensible for state=closed
    :returns: list of githubPRs
    """
    url = self._github( "pulls?state=%s&per_page=%s" % (state, perPage) )
    prs = curl2Json( ghHeaders(), url )

    if not mergedOnly:
      return prs

    ## only merged PRs
    prsToReturn = []
    for pr in prs:
      if pr.get( 'merged_at', None ) is not None:
        prsToReturn.append(pr)

    return prsToReturn


  def getNotesFromPRs( self, prs ):
    """ Loop over prs, get base branch, get PR comment and collate into dict of branch:dict( #PRID, comment ) """

    rawReleaseNotes = defaultdict( dict )

    for pr in prs:
      baseBranch = pr['base']['label'][len("DiracGrid:"):]
      if baseBranch not in self.branches:
        continue
      comment = parseForReleaseNotes( pr['body'] )
      prID = pr['number']
      mergeDate = pr.get('merged_at', None)
      mergeDate = mergeDate if mergeDate is not None else '9999-99-99'
      if mergeDate[:10] < self.startDate:
        continue

      rawReleaseNotes[baseBranch].update( {prID: dict(comment=comment, mergeDate=mergeDate)} )

    return rawReleaseNotes


  def getReleaseNotes( self ):

    if self.openPRs:
      prs = self.getGithubPRs( state='open', mergedOnly=False)
    else:
      prs = self.getGithubPRs( state='closed', mergedOnly=True)
    prs = self.getNotesFromPRs( prs )
    releaseNotes = collateReleaseNotes( prs )
    print releaseNotes


if __name__ == "__main__":
  RUNNER = GithubInterface()
  try:
    RUNNER.parseOptions()
  except RuntimeError as e:
    print ("Error during runtime: %s", e)
    exit(1)

  try:
    RUNNER.getReleaseNotes()
  except RuntimeError as e:
    exit(1)
