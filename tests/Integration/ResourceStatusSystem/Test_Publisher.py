""" This is a test of the PublisherHandler

    It supposes that the RSS DBs are present, and that the service is running
"""

# pylint: disable=invalid-name,wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.PublisherClient import PublisherClient

publisher = PublisherClient()
gLogger.setLevel('DEBUG')


def test_Get():
  res = publisher.getSites()
  assert res['OK'] is True, res['Message']

  res = publisher.getSitesResources(None)
  assert res['OK'] is True, res['Message']

  res = publisher.getElementStatuses('Site', None, None, None, None, None)
  assert res['OK'] is True, res['Message']

  res = publisher.getElementHistory('Site', None, None, None)
  assert res['OK'] is True, res['Message']

  res = publisher.getElementPolicies('Site', None, None)
  assert res['OK'] is True, res['Message']

  res = publisher.getNodeStatuses()
  assert res['OK'] is True, res['Message']

  res = publisher.getTree('', '')
  assert res['OK'] is True, res['Message']
