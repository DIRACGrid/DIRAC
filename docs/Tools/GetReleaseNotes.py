#!/bin/env python
""" script to obtain release notes from DIRAC PRs
"""

from collections import defaultdict
from datetime import datetime, timedelta
import argparse
from pprint import pformat
import logging
import textwrap
import requests

try:
  from GitTokens import GITHUBTOKEN
except ImportError:
  raise ImportError(textwrap.dedent("""
                    ***********************
                    Failed to import GITHUBTOKEN please!
                    Point the pythonpath to your GitTokens.py file which contains
                    your "Personal Access Token" for Github

                    I.e.:
                    Filename: GitTokens.py
                    Content:
                    ```
                    GITHUBTOKEN = "e0b83063396fc632646603f113437de9"
                    ```
                    (without the triple quotes)
                    ***********************
                    """),
                    )

SESSION = requests.Session()
SESSION.headers.update({'Authorization': "token %s " % GITHUBTOKEN})

logging.basicConfig(level=logging.WARNING, format='%(levelname)-5s - %(name)-8s: %(message)s')
LOGGER = logging.getLogger('GetReleaseNotes')


def req2Json(url, parameterDict=None, requestType='GET'):
  """Call to github API using requests package."""
  log = LOGGER.getChild("Requests")
  log.debug("Running %s with %s ", requestType, parameterDict)
  req = getattr(SESSION, requestType.lower())(url, json=parameterDict)
  if req.status_code not in (200, 201):
    log.error("Unable to access API: %s", req.text)
    raise RuntimeError("Failed to access API")

  log.debug("Result obtained:\n %s", pformat(req.json()))
  return req.json()


def getCommands(*args):
  """Create a flat list.

  :param *args: list of strings or tuples/lists
  :returns: flattened list of strings
  """
  comList = []
  for arg in args:
    if isinstance(arg, (tuple, list)):
      comList.extend(getCommands(*arg))
    else:
      comList.append(arg)
  return comList


def checkRate():
  """Return the result for check_rate call."""
  rate = req2Json(url="https://api.github.com/rate_limit")
  LOGGER.getChild("Rate").info("Remaining calls to github API are %s of %s",
                               rate['rate']['remaining'], rate['rate']['limit'])


def _parsePrintLevel(level):
  """Translate debug count to logging level."""
  level = level if level <= 2 else 2
  return [logging.WARNING,
          logging.INFO,
          logging.DEBUG,
          ][level]


def getFullSystemName(name):
  """Translate abbreviations to full system names."""
  name = {'API': 'Interfaces',
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
          }.get(name, name)
  return name


def parseForReleaseNotes(commentBody):
  """Look for "BEGINRELEASENOTES / ENDRELEASENOTES" and extend releaseNoteList if there are entries."""
  if not all(tag in commentBody for tag in ("BEGINRELEASENOTES", "ENDRELEASENOTES")):
    return ''
  return commentBody.split("BEGINRELEASENOTES")[1].split("ENDRELEASENOTES")[0]


def collateReleaseNotes(prs):
  """Put the release notes in the proper order.

  FIXME: Tag numbers could be obtained by getting the last tag with a name similar to
  the branch, will print out just the base branch for now.
  """
  releaseNotes = ""
  for baseBranch, pr in prs.iteritems():
    releaseNotes += "[%s]\n\n" % baseBranch
    systemChangesDict = defaultdict(list)
    for prid, content in pr.iteritems():
      notes = content['comment']
      system = ''
      for line in notes.splitlines():
        line = line.strip()
        if line.startswith("*"):
          system = getFullSystemName(line.strip("*:").strip())
        elif line:
          splitline = line.split(":", 1)
          if splitline[0] == splitline[0].upper() and len(splitline) > 1:
            line = "%s: (#%s) %s" % (splitline[0], prid, splitline[1].strip())
          systemChangesDict[system].append(line)

    for system, changes in systemChangesDict.iteritems():
      if system:
        releaseNotes += "*%s\n\n" % system
      releaseNotes += "\n".join(changes)
      releaseNotes += "\n\n"
    releaseNotes += "\n"

  return releaseNotes


class GithubInterface(object):
  """Object to make calls to github API."""

  def __init__(self, owner='DiracGrid', repo='Dirac'):
    """Set default values to parse release notes for DIRAC."""
    self.owner = owner
    self.repo = repo
    self.branches = ['Integration', 'rel-v6r19', 'rel-v6r20']
    self.openPRs = False
    self.startDate = str(datetime.now() - timedelta(days=14))[:10]
    self.printLevel = logging.WARNING
    LOGGER.setLevel(self.printLevel)

  @property
  def _options(self):
    """Return options dictionary."""
    return dict(owner=self.owner, repo=self.repo)

  def parseOptions(self):
    """Parse the command line options."""
    log = LOGGER.getChild('Options')
    parser = argparse.ArgumentParser("Dirac Release Notes",
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--branches", action="store", default=self.branches,
                        dest="branches", nargs='+',
                        help="branches to get release notes for")

    parser.add_argument("--date", action="store", default=self.startDate, dest="startDate",
                        help="date after which PRs are checked, default (two weeks ago): %s" % self.startDate)

    parser.add_argument("--openPRs", action="store_true", dest="openPRs", default=self.openPRs,
                        help="get release notes for open (unmerged) PRs, for testing purposes")

    parser.add_argument("-d", "--debug", action="count", dest="debug", help="d, dd, ddd", default=0)

    parser.add_argument("-r", "--repo", action="store", dest="repo", help="Repository to check: [Group/]Repo",
                        default='DiracGrid/Dirac')

    parsed = parser.parse_args()

    self.printLevel = _parsePrintLevel(parsed.debug)
    LOGGER.setLevel(self.printLevel)

    self.branches = parsed.branches
    log.info('Getting PRs for: %s', self.branches)
    self.startDate = parsed.startDate
    log.info('Starting from: %s', self.startDate)
    self.openPRs = parsed.openPRs
    log.info('Also including openPRs?: %s', self.openPRs)

    repo = parsed.repo
    repos = repo.split('/')
    if len(repos) == 1:
      self.repo = repo
    elif len(repos) == 2:
      self.owner = repos[0]
      self.repo = repos[1]
    else:
      raise RuntimeError("Cannot parse repo option: %s" % repo)

  def _github(self, action):
    """Return the url to perform actions on github.

    :param str action: command to use in the gitlab API, see documentation there
    :returns: url to be used
    """
    log = LOGGER.getChild('GitHub')
    options = dict(self._options)
    options["action"] = action
    ghURL = "https://api.github.com/repos/%(owner)s/%(repo)s/%(action)s" % options
    log.debug('Calling: %s', ghURL)
    return ghURL

  def getGithubPRs(self, state="open", mergedOnly=False, perPage=100):
    """Get all PullRequests from github.

    :param str state: state of the PRs, open/closed/all, default open
    :param bool merged: if PR has to be merged, only sensible for state=closed
    :returns: list of githubPRs
    """
    url = self._github("pulls?state=%s&per_page=%s" % (state, perPage))
    prs = req2Json(url=url)

    if not mergedOnly:
      return prs

    # only merged PRs
    prsToReturn = []
    for pr in prs:
      if pr.get('merged_at', None) is not None:
        prsToReturn.append(pr)

    return prsToReturn

  def getNotesFromPRs(self, prs):
    """Loop over prs, get base branch, get PR comment and collate into dictionary.

    :returns: dict of branch:dict(#PRID, dict(comment, mergeDate))
    """
    rawReleaseNotes = defaultdict(dict)

    for pr in prs:
      baseBranch = pr['base']['label'][len("DiracGrid:"):]
      if baseBranch not in self.branches:
        continue
      comment = parseForReleaseNotes(pr['body'])
      prID = pr['number']
      mergeDate = pr.get('merged_at', None)
      mergeDate = mergeDate if mergeDate is not None else '9999-99-99'
      if mergeDate[:10] < self.startDate:
        continue

      rawReleaseNotes[baseBranch].update({prID: dict(comment=comment, mergeDate=mergeDate)})

    return rawReleaseNotes

  def getReleaseNotes(self):
    """Create the release notes."""
    if self.openPRs:
      prs = self.getGithubPRs(state='open', mergedOnly=False)
    else:
      prs = self.getGithubPRs(state='closed', mergedOnly=True)
    prs = self.getNotesFromPRs(prs)
    releaseNotes = collateReleaseNotes(prs)
    print releaseNotes
    checkRate()


if __name__ == "__main__":

  RUNNER = GithubInterface()
  try:
    RUNNER.parseOptions()
  except RuntimeError as e:
    LOGGER.error("Error during argument parsing: %s", e)
    exit(1)

  try:
    RUNNER.getReleaseNotes()
  except RuntimeError as e:
    LOGGER.error("Error during runtime: %s", e)
    exit(1)
