"""
Test backend attachment
"""

__RCSID__ = "$Id$"

import pytest

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, gLoggerReset, cleaningLog


def getContentFromFilename(backendOptions):
  """ Get content from the file attached to a given backend and erase the content from the file.
  """
  filename = backendOptions.get('FileName')
  if not filename:
    return None

  # get the content of the file
  with open(filename, 'r') as fileContent:
    content = fileContent.read()

  # clean the content
  lines = content.split('\n')
  cleanContent = ''
  for line in lines:
    cleanContent += cleaningLog(line)

  # reset the file
  with open(filename, 'w') as fileContent:
    pass

  return cleanContent


@pytest.mark.parametrize("backends", [
    ({'file1': {'logger': 'gLogger',
                'backendType': 'file',
                'backendOptions': {'FileName': 'backend_test1.tmp'},
                'extractBackendContent': getContentFromFilename,
                'backendContent': 'Framework NOTICE: msgFramework/log NOTICE: msgFramework/log/sublog NOTICE: msg'}}),
    ({'file2': {'logger': 'log',
                'backendType': 'file',
                'backendOptions': {'FileName': 'backend_test1.tmp'},
                'extractBackendContent': getContentFromFilename,
                'backendContent': 'Framework/log NOTICE: msgFramework/log/sublog NOTICE: msg'}}),
    ({'file3': {'logger': 'sublog',
                'backendType': 'file',
                'backendOptions': {'FileName': 'backend_test1.tmp'},
                'extractBackendContent': getContentFromFilename,
                'backendContent': 'Framework/log/sublog NOTICE: msg'}}),
    ({'file4': {'logger': 'gLogger',
                'backendType': 'file',
                'backendOptions': {'FileName': 'backend_test1.tmp', 'LogLevel': 'error'},
                'extractBackendContent': getContentFromFilename,
                'backendContent': ''}}),
    ({'file5': {'logger': 'log',
                'backendType': 'file',
                'backendOptions': {'FileName': 'backend_test1.tmp', 'LogLevel': 'error'},
                'extractBackendContent': getContentFromFilename,
                'backendContent': ''}}),
    ({'file6': {'logger': 'sublog',
                'backendType': 'file',
                'backendOptions': {'FileName': 'backend_test1.tmp', 'LogLevel': 'error'},
                'extractBackendContent': getContentFromFilename,
                'backendContent': ''}}),
    ({'file7a': {'logger': 'gLogger',
                 'backendType': 'file',
                 'backendOptions': {'FileName': 'backend_test1.tmp'},
                 'extractBackendContent': getContentFromFilename,
                 'backendContent': 'Framework NOTICE: msgFramework/log NOTICE: msgFramework/log/sublog NOTICE: msg'},
      'file7b': {'logger': 'gLogger',
                 'backendType': 'file',
                 'backendOptions': {'FileName': 'backend_test2.tmp'},
                 'extractBackendContent': getContentFromFilename,
                 'backendContent': 'Framework NOTICE: msgFramework/log NOTICE: msgFramework/log/sublog NOTICE: msg'}}),
])
def test_registerBackendgLogger(backends):
  """
  Attach backends to gLogger, generate some logs from different loggers and check the content of the backends
  """
  _, log, sublog = gLoggerReset()

  # dictionary of available loggers
  loggers = {'gLogger': gLogger, 'log': log, 'sublog': sublog}

  # attach backends to the corresponding logger
  for backend, params in backends.items():
    logger = loggers[params['logger']]
    numberOfBackends = len(logger._backendsList)
    logger.registerBackend(params['backendType'], params['backendOptions'])

    # backend should be added to logger.backendList
    assert len(logger._backendsList) == (numberOfBackends + 1)

  # Generate logs from gLogger, log, sublog
  gLogger.setLevel('notice')
  gLogger.notice('msg')
  log.notice('msg')
  sublog.notice('msg')

  # Check the content of the backends
  for backend, params in backends.items():
    content = params['extractBackendContent'](params['backendOptions'])
    assert content == params['backendContent']
