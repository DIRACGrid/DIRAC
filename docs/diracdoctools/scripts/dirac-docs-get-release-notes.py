#!/usr/bin/env python
"""Script to obtain release notes from PRs for DIRAC or other packages

Use the command line parameters or a config file to configure the options.
The long names used on the command line can also be used in the config file

To pick up release notes from gitlab follow this example

  [ReleaseNotes]
  gitlabProjectID = 320
  gitlab = True
  branches = Rel-v29r0, master
  date = 2019-01-01
  gitlabUrl = https://gitlab.cern.ch

To extend the coverter list of short system names to their full equivalent beyond what is configured for DIRAC
add this section to the config file

  [ReleaseNotes.Categories]
  ITS = ILCTransformationSystem
  CI = Testing
  All = Core

"""

import os
from collections import defaultdict
import datetime
import dateutil.parser
import pytz
import argparse
from pprint import pformat
import logging
import textwrap
import requests
from distutils.version import LooseVersion
from configparser import ConfigParser


G_ERROR = textwrap.dedent(
    """
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
                          """
)

SESSION = requests.Session()

logging.basicConfig(level=logging.WARNING, format="%(levelname)-5s - %(name)-8s: %(message)s")
LOGGER = logging.getLogger("GetReleaseNotes")


def listify(values):
    """Turn a comma separated string into a list."""
    if isinstance(values, list):
        return values
    return [entry.strip() for entry in values.split(",") if entry]


def githubSetup(GITHUBTOKEN=""):
    """Import the GITHUB Token and add proper header."""
    LOGGER.info("Setting up GITHUB")
    if not GITHUBTOKEN:
        try:
            from GitTokens import GITHUBTOKEN
        except ImportError:
            raise ImportError(G_ERROR)
    if GITHUBTOKEN:
        SESSION.headers.update({"Accept": "application/vnd.github.v3+json", "Authorization": f"token {GITHUBTOKEN} "})


def gitlabSetup(GITLABTOKEN=""):
    """Import the GITLAB Token and add proper header."""
    LOGGER.info("Setting up GitLab")
    if not GITLABTOKEN:
        try:
            from GitTokens import GITLABTOKEN
        except ImportError:
            raise ImportError(G_ERROR)
    if GITLABTOKEN:
        SESSION.headers.update({"PRIVATE-TOKEN": GITLABTOKEN})


def req2Json(url, parameterDict=None, requestType="GET"):
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


def _parsePrintLevel(level):
    """Translate debug count to logging level."""
    level = level if level <= 2 else 2
    return [
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
    ][level]


def parseForReleaseNotes(commentBody):
    """Look for "BEGINRELEASENOTES / ENDRELEASENOTES" and extend releaseNoteList if there are entries."""
    commentBody = commentBody or ""
    if not all(tag in commentBody for tag in ("BEGINRELEASENOTES", "ENDRELEASENOTES")):
        return ""
    return commentBody.split("BEGINRELEASENOTES")[1].split("ENDRELEASENOTES")[0]


class GithubInterface:
    """Object to make calls to github API."""

    def __init__(self, owner="DiracGrid", repo="Dirac"):
        """Set default values to parse release notes for DIRAC."""
        self.owner = owner
        self.repo = repo
        self.branches = ["Integration", "rel-v6r21"]
        self.openPRs = False
        self.sinceLatestTag = False
        self.sinceTag = None
        self.headerMessage = None
        self.footerMessage = None
        self.startDate = datetime.datetime.now() - datetime.timedelta(days=14)
        self.token = None
        self.printLevel = logging.WARNING
        logging.getLogger().setLevel(self.printLevel)
        self.useGitlab = False
        self.useGithub = True
        self.deployRelease = False
        self.releaseNotes = None
        self.tagName = None

        self.gitlabUrl = "https://gitlab.cern.ch"
        self.glProjectID = 0

        # translate abbreviations to full system names.
        self.names = {
            "API": "Interfaces",
            "AS": "AccountingSystem",
            "CS": "ConfigurationSystem",
            "Config": "ConfigurationSystem",
            "Configuration": "ConfigurationSystem",
            "DMS": "DataManagementSystem",
            "DataManagement": "DataManagementSystem",
            "FS": "FrameworkSystem",
            "Framework": "FrameworkSystem",
            "MS": "MonitoringSystem",
            "Monitoring": "MonitoringSystem",
            "RMS": "RequestManagementSystem",
            "RequestManagement": "RequestManagementSystem",
            "RSS": "ResourceStatusSystem",
            "ResourceStatus": "ResourceStatusSystem",
            "SMS": "StorageManagamentSystem",
            "StorageManagement": "StorageManagamentSystem",
            "TS": "TransformationSystem",
            "TMS": "TransformationSystem",
            "Transformation": "TransformationSystem",
            "WMS": "WorkloadManagementSystem",
            "Workload": "WorkloadManagementSystem",
        }

    def getFullSystemName(self, name):
        """Return full name based on abbrevation for system."""
        return self.names.get(name, name)

    @property
    def _options(self):
        """Return options dictionary."""
        return dict(owner=self.owner, repo=self.repo)

    def checkRate(self):
        """Return the result for check_rate call."""
        if self.useGitlab:
            return
        rate = req2Json(url="https://api.github.com/rate_limit")
        LOGGER.getChild("Rate").info(
            "Remaining calls to github API are %s of %s", rate["rate"]["remaining"], rate["rate"]["limit"]
        )

    def parseOptions(self):
        """Parse the command line options."""
        log = LOGGER.getChild("Options")

        conf_parser = argparse.ArgumentParser(
            "Dirac Release Notes",
            formatter_class=argparse.RawTextHelpFormatter,
            add_help=False,
        )
        conf_parser.add_argument("-c", "--configFile", help="Specify config file", metavar="FILE", dest="configFile")
        conf_parser.add_argument("-t", "--token", help="API token to use", default="")
        conf_parser.add_argument("-d", "--debug", action="count", dest="debug", help="d, dd, ddd", default=0)
        args, _remaining_argv = conf_parser.parse_known_args()
        self.token = args.token
        self.printLevel = _parsePrintLevel(args.debug)
        LOGGER.setLevel(self.printLevel)
        log.debug("Parsing options")
        defaults = {}
        if args.configFile:
            log.debug("Parsing configFile: %r", args.configFile)
            config = ConfigParser()
            config.optionxform = str
            config.read([args.configFile])
            defaults.update(dict(config.items("ReleaseNotes")))
            log.info("Settings from config file: %r", defaults)

            if config.has_section("ReleaseNotes.Categories"):
                items = config.items("ReleaseNotes.Categories")
                log.info("Found Categories: %r", items)
                for short, system in items:
                    self.names[short] = system

        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            # Inherit options from config_parser
            parents=[conf_parser],
        )
        parser.add_argument(
            "--branches",
            action="store",
            default=self.branches,
            dest="branches",
            nargs="+",
            help="branches to get release notes for",
        )

        parser.add_argument(
            "--date",
            action="store",
            default=self.startDate,
            dest="date",
            help="date and optionally time after which PRs are checked (ISO 8601),\
                         accepting 2020-01-08 or 2018-05-20T15:23:45Z,\
                         default (two weeks ago): %s"
            % self.startDate,
        )

        parser.add_argument(
            "--sinceLatestTag",
            action="store_true",
            dest="sinceLatestTag",
            default=self.sinceLatestTag,
            help="get release notes since latest tag (incompatible with --date)",
        )

        parser.add_argument(
            "--sinceTag",
            dest="sinceTag",
            default=self.sinceTag,
            help="get release notes since latest tag (incompatible with --date)",
        )

        parser.add_argument(
            "--headerMessage",
            action="store",
            default=self.headerMessage,
            dest="headerMessage",
            help="Header message to add between the release name and the list of PR. If it is a path,\
                         read the content of the file",
        )

        parser.add_argument(
            "--footerMessage",
            action="store",
            default=self.footerMessage,
            dest="footerMessage",
            help="Footer message to add after the list of PR. If it is a path,\
                      read the content of the file",
        )

        parser.add_argument(
            "--openPRs",
            action="store_true",
            dest="openPRs",
            default=self.openPRs,
            help="get release notes for open (unmerged) PRs, for testing purposes",
        )

        parser.add_argument(
            "-r",
            "--repo",
            action="store",
            dest="repo",
            help="Github repository to check: [Group/]Repo",
            default="DiracGrid/Dirac",
        )

        parser.add_argument(
            "-g", "--gitlab", action="store_true", dest="gitlab", help="Using gitlab instance", default=False
        )

        parser.add_argument(
            "-u",
            "--gitlabUrl",
            action="store",
            dest="gitlabUrl",
            help="URL of the gitlab instance",
            default="https://gitlab.cern.ch",
        )

        parser.add_argument(
            "-i",
            "--gitlabProjectID",
            action="store",
            dest="gitlabProjectID",
            help="ID of the project in Gitlab",
            default="0",
        )

        parser.add_argument(
            "--deployRelease",
            action="store_true",
            dest="deployRelease",
            help="Convert an existing tag into a github/gitlab release. Requires --releaseNotes and --tagName",
            default=self.deployRelease,
        )

        parser.add_argument(
            "--tagName",
            action="store",
            dest="tagName",
            help="Name of the tag to release (with --deployRelease)",
            default=self.tagName,
        )

        parser.add_argument(
            "--releaseNotes",
            action="store",
            dest="releaseNotes",
            help="Path to the file containing release notes for this version (with --deployRelease)",
            default=self.releaseNotes,
        )

        parser.set_defaults(**defaults)

        parsed = parser.parse_args()

        for var, val in sorted(vars(parsed).items()):
            log.info("Parsed options: %s = %s", var, pformat(val))

        self.branches = listify(parsed.branches)
        log.info("Getting PRs for: %s", self.branches)

        # If the date parsed does not correspond to the default,
        # and latestTag is asked, we throw an error
        if (bool(parsed.date != self.startDate) + bool(parsed.sinceLatestTag) + bool(parsed.sinceTag)) > 1:
            raise RuntimeError("--sinceLatestTag, --date and --sinceTag are mutually exclusive")

        self.sinceLatestTag = parsed.sinceLatestTag
        self.sinceTag = parsed.sinceTag

        if self.sinceLatestTag:
            log.info("Starting from the latest tag")
            self.startDate = None
            del parsed.date
        else:
            if not isinstance(parsed.date, datetime.date):
                parsed.date = dateutil.parser.isoparse(parsed.date)
            if parsed.date.tzinfo is None or parsed.date.tzinfo.utcoffset(parsed.date) is None:
                self.startDate = pytz.utc.localize(parsed.date)
            else:
                self.startDate = parsed.date
            log.info("Starting from: %s", self.startDate)

        self.openPRs = parsed.openPRs
        log.info("Also including openPRs?: %s", self.openPRs)

        self.headerMessage = parsed.headerMessage
        if self.headerMessage:
            log.info("Using header message: %s", self.headerMessage)

        self.footerMessage = parsed.footerMessage
        if self.footerMessage:
            log.info("Using footer message: %s", self.footerMessage)

        self.useGitlab = parsed.gitlab if isinstance(parsed.gitlab, bool) else parsed.gitlab.lower() == "true"
        self.useGithub = not self.useGitlab

        self.gitlabUrl = parsed.gitlabUrl
        self.glProjectID = int(parsed.gitlabProjectID)

        self.deployRelease = parsed.deployRelease
        self.releaseNotes = parsed.releaseNotes
        self.tagName = parsed.tagName

        if self.deployRelease:
            if not (self.releaseNotes and self.tagName):
                raise RuntimeError("--deployRelease requires --releaseNotes and --tagName")
            if not os.path.isfile(self.releaseNotes):
                raise RuntimeError("--releaseNotes should point to an existing file")

        repo = parsed.repo
        repos = repo.split("/")
        if len(repos) == 1:
            self.repo = repo
        elif len(repos) == 2:
            self.owner = repos[0]
            self.repo = repos[1]
        else:
            raise RuntimeError(f"Cannot parse repo option: {repo}")

        for var, val in sorted(vars(parsed).items()):
            log.info("Using options: %s = %s", var, pformat(val))

    def _github(self, action):
        """Return the url to perform actions on github.

        :param str action: command to use in the gitlab API, see documentation there
        :returns: url to be used
        """
        log = LOGGER.getChild("GitHub")
        options = dict(self._options)
        options["action"] = action
        ghURL = f"https://api.github.com/repos/{options['owner']}/{options['repo']}/{options['action']}"
        log.debug("Calling: %s", ghURL)
        return ghURL

    def _gitlab(self, action):
        """Return URL for gitlab using proper ID and action needed

        :param str action: command to use in the gitlab API, see documentation there
        :returns: url to be used by curl
        """
        return "%s/api/v4/projects/%d/%s" % (self.gitlabUrl, self.glProjectID, action)

    def getGitlabPRs(self, state="opened"):
        """Get PRs in the gitlab repository."""
        glURL = self._gitlab(f"merge_requests?state={state}")
        return req2Json(glURL)

    def getGithubPRs(self, state="open", mergedOnly=False, perPage=100):
        """Get all PullRequests from github.

        :param str state: state of the PRs, open/closed/all, default open
        :param bool merged: if PR has to be merged, only sensible for state=closed
        :returns: list of githubPRs
        """
        url = self._github(f"pulls?state={state}&per_page={perPage}")
        prs = req2Json(url=url)

        if not mergedOnly:
            return prs

        # only merged PRs
        prsToReturn = []
        for pr in prs:
            if pr.get("merged_at", None) is not None:
                prsToReturn.append(pr)

        return prsToReturn

    def getGitlabLatestTagDate(self, sinceTag):
        """Get the latest tag creation date from gitlab

        :param str sinceTag: Use this tag as a reference instead of the latest
        :returns: date of the latest tag
        """
        if sinceTag:
            raise NotImplementedError()
        glURL = self._gitlab("repository/tags")
        allTags = req2Json(glURL)
        lastTag = max(tag["commit"]["created_at"] for tag in allTags)
        return dateutil.parser.isoparse(lastTag)

    def getGithubLatestTagDate(self, sinceTag):
        """Get the latest tag creation date from gitlab

        :warning: tags can only be sorted by name, so we assume that the tags are ordered version numbers

        :param str sinceTag: Use this tag as a reference instead of the latest
        :returns: date of the latest tag
        """
        log = LOGGER.getChild("getGithubLatestTagDate")

        # Get all tags
        tags = req2Json(url=self._github("tags"))
        if isinstance(tags, dict) and "Not Found" in tags.get("message"):
            raise RuntimeError(f"Package not found: {str(self)}")

        if sinceTag:
            for tag in tags:
                if tag["name"] == sinceTag:
                    latestTag = tag
                    break
            else:
                raise ValueError(f"Tag {sinceTag} not found")
        else:
            sortedTags = sorted(tags, key=lambda tag: LooseVersion(tag["name"]), reverse=True)
            latestTag = sortedTags[0]

        log.info("Found latest tag %s", latestTag["name"])

        # Use the sha of the commit to finally retrieve the date
        latestTagCommitSha = latestTag["commit"]["sha"]
        commitInfo = req2Json(url=self._github(f"git/commits/{latestTagCommitSha}"))

        startDate = dateutil.parser.isoparse(commitInfo["committer"]["date"])

        log.info("Found latest tag date %s", startDate)

        return startDate

    def getNotesFromPRs(self, prs):
        """Loop over prs, get base branch, get PR comment and collate into dictionary.

        :returns: dict of branch:dict(#PRID, dict(comment, mergeDate))
        """
        rawReleaseNotes = defaultdict(dict)
        for pr in prs:
            if self.useGithub:
                baseBranch = pr["base"]["label"][len(self.owner) + 1 :]
                if baseBranch not in self.branches:
                    continue
                comment = parseForReleaseNotes(pr["body"])
                prID = pr["number"]

            if self.useGitlab:
                baseBranch = pr["target_branch"]
                if baseBranch not in self.branches:
                    continue
                comment = parseForReleaseNotes(pr["description"])
                prID = pr["iid"]

            mergeDate = pr.get("merged_at", None)
            mergeDate = mergeDate if mergeDate is not None else "9999-99-99"
            mergeDate = dateutil.parser.isoparse(mergeDate)
            if mergeDate < self.startDate:
                continue
            rawReleaseNotes[baseBranch].update({prID: dict(comment=comment, mergeDate=mergeDate)})

        return rawReleaseNotes

    def getReleaseNotes(self):
        """Create the release notes."""

        log = LOGGER.getChild("getReleaseNotes")

        # Check the latest tag if need be
        if self.sinceLatestTag or self.sinceTag:
            if self.useGithub:
                self.startDate = self.getGithubLatestTagDate(self.sinceTag)
            else:
                self.startDate = self.getGitlabLatestTagDate(self.sinceTag)
            log.info("Starting from date %s", self.startDate)

        if self.useGithub:
            if self.openPRs:
                prs = self.getGithubPRs(state="open", mergedOnly=False)
            else:
                prs = self.getGithubPRs(state="closed", mergedOnly=True)
        elif self.useGitlab:
            if self.openPRs:
                prs = self.getGitlabPRs(state="all")
            else:
                prs = self.getGitlabPRs(state="merged")

        prs = self.getNotesFromPRs(prs)
        releaseNotes = self.collateReleaseNotes(prs)
        print(releaseNotes)
        self.checkRate()

    def collateReleaseNotes(self, prs):
        """Put the release notes in the proper order.

        FIXME: Tag numbers could be obtained by getting the last tag with a name similar to
        the branch, will print out just the base branch for now.
        """
        releaseNotes = ""

        headerMessage = self.headerMessage
        # If the headerMessage option passed is a file, read the content
        if self.headerMessage and os.path.isfile(self.headerMessage):
            with open(self.headerMessage) as hmf:
                headerMessage = hmf.read()

        footerMessage = self.footerMessage
        # If the footerMessage option passed is a file, read the content
        if self.footerMessage and os.path.isfile(self.footerMessage):
            with open(self.footerMessage) as hmf:
                footerMessage = hmf.read()

        # If there are no CHANGELOGS, which can happen
        # just add the header/footer
        # CAUTION: no [tag] will be written
        if not prs:
            if headerMessage:
                releaseNotes += f"{headerMessage}\n\n"
            if footerMessage:
                releaseNotes += f"{footerMessage}\n"
            return releaseNotes

        prMarker = "#" if self.useGithub else "!"
        for baseBranch, pr in prs.items():
            releaseNotes += f"[{baseBranch}]\n\n"
            if headerMessage:
                releaseNotes += f"{headerMessage}\n\n"
            systemChangesDict = defaultdict(list)
            for prid, content in pr.items():
                notes = content["comment"]
                system = ""
                for line in notes.splitlines():
                    line = line.strip()
                    if line.startswith("*"):
                        system = self.getFullSystemName(line.strip("*:").strip())
                    elif line:
                        splitline = line.split(":", 1)
                        if splitline[0] == splitline[0].upper() and len(splitline) > 1:
                            line = f"{splitline[0]}: ({prMarker}{prid}) {splitline[1].strip()}"
                        systemChangesDict[system].append(line)

            for system, changes in systemChangesDict.items():
                if system:
                    releaseNotes += f"*{system}\n\n"
                releaseNotes += "\n".join(changes)
                releaseNotes += "\n\n"
            releaseNotes += "\n"

            if footerMessage:
                releaseNotes += f"\n{footerMessage}\n"

        return releaseNotes

    def setup(self):
        """Setup the API"""
        # Setting up the API
        if self.useGithub:
            githubSetup(self.token)
        elif self.useGitlab:
            gitlabSetup(self.token)

    def createGithubRelease(self):
        """make a release on github"""

        log = LOGGER.getChild("createGithubRelease")

        log.info("Creating a release for github")
        with open(self.releaseNotes) as rnf:
            releaseNotes = rnf.read()

        releaseDict = dict(
            tag_name=self.tagName,
            name=self.tagName,
            body=releaseNotes,
            prerelease=False,
            draft=False,
        )

        log.debug("Release dict %s", releaseDict)

        result = req2Json(url=self._github("releases"), parameterDict=releaseDict, requestType="POST")

        log.info("Result %s", result)

        return result

    def createGitlabRelease(self):
        """make a release on gitlab"""

        log = LOGGER.getChild("createGitlabRelease")

        log.info("Creating a release for gitlab")
        with open(self.releaseNotes) as rnf:
            releaseNotes = "\n".join(rnf.readlines())

        if not releaseNotes:
            releaseNotes = "No changes"

        releaseDict = dict(
            id=self.glProjectID,
            tag_name=self.tagName,
            name=self.tagName,
            description=releaseNotes,
        )

        log.debug("Release dict %s", releaseDict)

        result = req2Json(url=self._gitlab("releases"), parameterDict=releaseDict, requestType="POST")

        log.info("Result %s", result)
        return result

    def createRelease(self):
        """Convert an existing github/gitlab tag into a release"""

        if self.useGithub:
            return self.createGithubRelease()
        elif self.useGitlab:
            return self.createGitlabRelease()


if __name__ == "__main__":

    RUNNER = GithubInterface()
    try:
        RUNNER.parseOptions()
    except RuntimeError as e:
        LOGGER.error("Error during argument parsing: %s", e)
        exit(1)

    RUNNER.setup()

    try:
        # If it is invoked to deploy the release
        if RUNNER.deployRelease:
            RUNNER.createRelease()
        # or to generate the release notes
        else:
            RUNNER.getReleaseNotes()
    except RuntimeError as e:
        LOGGER.error("Error during runtime: %s", e)
        exit(1)
