""" This is a test of the chain
    ResourceStatus -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

gLogger.setLevel('DEBUG')

rssClient = ResourceStatus()

def test_addAndRemove():

  result = rssClient.setElementStatus("test_element", "StorageElement", "ReadAccess", "Banned")
  assert result['OK'] == True

  rssClient.rssCache.refreshCache()
  result = rssClient.getElementStatus("test_element", "StorageElement", "ReadAccess")
  assert result['OK'] == True
  assert result['Value']['test_element']['ReadAccess'] == 'Banned'

  result = rssClient.setElementStatus("test_element2", "ComputingElement", "all", "Banned")
  assert result['OK'] == True

  rssClient.rssCache.refreshCache()
  result = rssClient.getElementStatus("test_element2", "ComputingElement")
  assert result['OK'] == True
  assert result['Value']['test_element2']['all'] == 'Banned'