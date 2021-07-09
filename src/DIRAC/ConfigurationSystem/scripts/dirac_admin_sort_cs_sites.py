#!/usr/bin/env python
########################################################################
# File :    dirac-admin-sort-cs-sites
# Author :  Matvey Sapunov
########################################################################
"""
Sort site names at CS in "/Resources" section. Sort can be alphabetic or by country postfix in a site name.
Alphabetic sort is default (i.e. LCG.IHEP.cn, LCG.IHEP.su, LCG.IN2P3.fr)

Example:
  $ dirac-admin-sort-cs-sites -C CLOUDS DIRAC
  sort site names by country postfix in '/Resources/Sites/CLOUDS' and '/Resources/Sites/DIRAC' subsection
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger, exit as DIRACExit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getPropertiesForGroup
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Core.Utilities.Time import dateTime, toString


class SortCSSites(DIRACScript):

  def initParameters(self):
    """ init """
    self.SORTBYNAME = True
    self.REVERSE = False

  def sortBy(self, arg):
    self.SORTBYNAME = False

  def isReverse(self, arg):
    self.REVERSE = True

  def country(self, arg):
    cb = arg.split(".")
    if not len(cb) == 3:
      gLogger.error("%s is not in GRID.NAME.COUNTRY format ")
      return False
    return cb[2]


@SortCSSites()
def main(self):
  self.registerSwitch(
      "C",
      "country",
      "Sort site names by country postfix (i.e. LCG.IHEP.cn, LCG.IN2P3.fr, LCG.IHEP.su)",
      self.sortBy)
  self.registerSwitch("R", "reverse", "Reverse the sort order", self.isReverse)
  self.registerArgument(["Section: Name of the subsection in '/Resources/Sites/' for sort (i.e. LCG DIRAC)"],
                        mandatory=False)
  _, args = self.parseCommandLine(ignoreErrors=True)

  result = getProxyInfo()
  if not result["OK"]:
    gLogger.error("Failed to get proxy information", result["Message"])
    DIRACExit(2)
  proxy = result["Value"]
  if proxy["secondsLeft"] < 1:
    gLogger.error("Your proxy has expired, please create new one")
    DIRACExit(2)
  group = proxy["group"]
  if "CSAdministrator" not in getPropertiesForGroup(group):
    gLogger.error("You must be CSAdministrator user to execute this script")
    gLogger.notice("Please issue 'dirac-proxy-init -g [group with CSAdministrator Property]'")
    DIRACExit(2)

  cs = CSAPI()
  result = cs.getCurrentCFG()
  if not result["OK"]:
    gLogger.error("Failed to get copy of CS", result["Message"])
    DIRACExit(2)
  cfg = result["Value"]

  if not cfg.isSection("Resources"):
    gLogger.error("Section '/Resources' is absent in CS")
    DIRACExit(2)

  if not cfg.isSection("Resources/Sites"):
    gLogger.error("Subsection '/Resources/Sites' is absent in CS")
    DIRACExit(2)

  if args and len(args) > 0:
    resultList = args[:]
  else:
    resultList = cfg["Resources"]["Sites"].listSections()

  hasRun = False
  isDirty = False
  for i in resultList:
    if not cfg.isSection("Resources/Sites/%s" % i):
      gLogger.error("Subsection /Resources/Sites/%s does not exists" % i)
      continue
    hasRun = True
    if self.SORTBYNAME:
      dirty = cfg["Resources"]["Sites"][i].sortAlphabetically(ascending=not self.REVERSE)
    else:
      dirty = cfg["Resources"]["Sites"][i].sortByKey(key=self.country, reverse=self.REVERSE)
    if dirty:
      isDirty = True

  if not hasRun:
    gLogger.notice("Failed to find suitable subsections with site names to sort")
    DIRACExit(0)

  if not isDirty:
    gLogger.notice("Nothing to do, site names are already sorted")
    DIRACExit(0)

  timestamp = toString(dateTime())
  stamp = "Site names are sorted by %s script at %s" % (self.scriptName, timestamp)
  cs.setOptionComment("/Resources/Sites", stamp)

  result = cs.commit()
  if not result["OK"]:
    gLogger.error("Failed to commit changes to CS", result["Message"])
    DIRACExit(2)
  gLogger.notice("Site names are sorted and committed to CS")
  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
