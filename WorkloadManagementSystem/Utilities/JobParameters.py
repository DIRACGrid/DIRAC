""" DIRAC Workload Management System utility module to get available memory and processors from mjf
"""

__RCSID__ = "$Id$"

import os
import re
import multiprocessing
from six.moves.urllib.request import urlopen

from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities.List import fromChar


def getJobFeatures():
  features = {}
  if 'JOBFEATURES' not in os.environ:
    return features
  for item in ('allocated_cpu', 'hs06_job', 'shutdowntime_job', 'grace_secs_job', 'jobstart_secs',
               'job_id', 'wall_limit_secs',
               'cpu_limit_secs', 'max_rss_bytes', 'max_swap_bytes', 'scratch_limit_bytes'):
    fname = os.path.join(os.environ['JOBFEATURES'], item)
    try:
      val = urlopen(fname).read()
    except BaseException:
      val = 0
    features[item] = val
  return features


def getProcessorFromMJF():
  return getJobFeatures().get('allocated_cpu')


def getMemoryFromMJF():
  return getJobFeatures().get('max_rss_bytes')


def getMemoryFromProc():
  meminfo = dict((i.split()[0].rstrip(':'), int(i.split()[1])) for i in open('/proc/meminfo').readlines())
  maxRAM = meminfo['MemTotal']
  if maxRAM:
    return maxRAM / 1024


def getNumberOfProcessors(siteName, gridCE, queue):
  """ gets the number of processors on a certain CE/queue/node

      Tries to find it in this order:
      1) from the /Resources/Computing/CEDefaults/NumberOfProcessors (which is what the pilot fill up)
      2) if not present from JobFeatures
      3) if not present looks in CS for "NumberOfProcessors" Queue or CE option
      4) if not present looks in CS for "%dProcessors" Queue or CE Tag
      5) if not present but there's WholeNode tag, look what the WN provides using multiprocessing.cpu_count()
      6) return 1
  """

  # 1) from /Resources/Computing/CEDefaults/NumberOfProcessors
  gLogger.info("Getting numberOfProcessors from /Resources/Computing/CEDefaults/NumberOfProcessors")
  numberOfProcessors = gConfig.getValue('/Resources/Computing/CEDefaults/NumberOfProcessors')
  if numberOfProcessors:
    return numberOfProcessors

  # 2) from MJF
  gLogger.info("Getting numberOfProcessors from MJF")
  numberOfProcessors = getProcessorFromMJF()
  if numberOfProcessors:
    return numberOfProcessors

  # 3) looks in CS for "NumberOfProcessors" Queue or CE or site option
  grid = siteName.split('.')[0]

  gLogger.info("NumberOfProcessors could not be found in MJF, trying from CS (queue definition)")
  numberOfProcessors = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/Queues/%s/NumberOfProcessors' % (grid,
                                                                                                        siteName,
                                                                                                        gridCE,
                                                                                                        queue))
  if numberOfProcessors:
    return numberOfProcessors

  gLogger.info("NumberOfProcessors could not be found in CS queue definition, ",
               "trying from /Resources/Sites/%s/%s/CEs/%s/NumberOfProcessors" % (grid, siteName, gridCE))
  numberOfProcessors = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/NumberOfProcessors' % (grid,
                                                                                              siteName,
                                                                                              gridCE))
  if numberOfProcessors:
    return numberOfProcessors

  gLogger.info("NumberOfProcessors could not be found in CS CE definition, ",
               "trying from /Resources/Sites/%s/%s/NumberOfProcessors" % (grid, siteName))
  numberOfProcessors = gConfig.getValue('/Resources/Sites/%s/%s/NumberOfProcessors' % (grid, siteName))
  if numberOfProcessors:
    return numberOfProcessors

  # 3) looks in CS for tags
  gLogger.info("Getting number of processors" "from tags for %s: %s: %s" % (siteName, gridCE, queue))
  # Tags of the CE
  tags = fromChar(gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/Tag' % (siteName.split('.')[0], siteName, gridCE),
                  ''))
  # Tags of the Queue
  tags += fromChar(gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/Queues/%s/Tag' % (siteName.split('.')[0],
                                                                                     siteName,
                                                                                     gridCE, queue),
                   ''))
  for tag in tags:
    numberOfProcessorsTag = re.search('[0-9]Processors', tag)
    if numberOfProcessorsTag:
      gLogger.info("Number of processors from tags", numberOfProcessorsTag.string)
      return int(numberOfProcessorsTag.string.replace('Processors', ''))

  gLogger.info("NumberOfProcessors could not be found in CS")
  if 'WholeNode' in tags:
    gLogger.info("Found WholeNode tag, using multiprocessing.cpu_count()")
    return multiprocessing.cpu_count()

  return 1
