""" Unit tests for the Dirac interface module
"""
# pylint: disable=no-member, protected-access, missing-docstring
import logging

from pprint import pformat
import pytest
from mock import MagicMock, call

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC import S_OK

logging.basicConfig()
LOG = logging.getLogger('TestDirac')
LOG.setLevel(logging.ERROR)


@pytest.fixture
def dirac():
  d = Dirac()
  d.log = MagicMock(name="Log")
  d.log.debug = d.log
  d.log.info = d.log
  d.log.notice = d.log
  d.log.verbose = d.log
  d.log.error = d.log
  d.log.warn = d.log
  return d


@pytest.fixture
def job():
  from DIRAC.Interfaces.API.Job import Job
  job = Job(stdout='printer', stderr='/dev/null')
  return job


@pytest.fixture
def osmock():
  os = MagicMock(return_value=False, name="OS")
  os.getcwd.return_value = '/root'
  os.environ = dict()

  def expandMock(*args, **kwargs):
    if 'DIRACROOT' in os.environ and \
       'DIRACROOT' in args[0]:
      return args[0].replace('$DIRACROOT', os.environ['DIRACROOT'])
    return args[0]
  os.path.expandvars.side_effect = expandMock
  return os


@pytest.fixture
def confMock():
  gConf = MagicMock(name="gConfig")

  def getVal(*args, **kwargs):
    if '/LocalSite/Root' in args[0]:
      return '/root/dirac'
    if len(args) == 2:
      return args[1]
    return 'defaultValue'
  gConf.getValue.side_effect = getVal
  return gConf


def test_basicJob(dirac):
  jdl = "Parameter=Value;Parameter2=Value2"
  ret = dirac._Dirac__getJDLParameters(jdl)
  assert ret['OK']
  assert 'Parameter' in ret['Value']
  assert ret['Value']['Parameter'] == 'Value'
  assert 'Parameter2' in ret['Value']
  assert ret['Value']['Parameter2'] == 'Value2'


def test_JobJob(dirac, job):
  ret = dirac._Dirac__getJDLParameters(job)
  assert ret['OK']
  assert ret['Value']['StdOutput'] == 'printer'
  assert ret['Value']['StdError'] == '/dev/null'


def test_runLocal(dirac, job, mocker, osmock, confMock):
  mocker.patch('DIRAC.Interfaces.API.Dirac.os', new=osmock)
  mocker.patch('DIRAC.Interfaces.API.Dirac.tempfile')
  mocker.patch('DIRAC.Interfaces.API.Dirac.shutil')
  mocker.patch('DIRAC.Interfaces.API.Dirac.gConfig', new=confMock)
  sysMock = mocker.patch('DIRAC.Interfaces.API.Dirac.systemCall')
  sysMock.return_value = S_OK([0, 'No output', 'No errors'])
  mocker.patch('DIRAC.Interfaces.API.Dirac.tarfile', new=MagicMock(return_value=False))
  mocker.patch('__builtin__.open')
  ret = dirac.runLocal(job)
  LOG.info("dirac log calls: %s", dirac.log.call_args_list)
  LOG.info("CallStack: %s", pformat(ret.get('CallStack', {})))
  assert sysMock.call_args_list[0][1]['cmdSeq'] == ['/root/dirac/scripts/dirac-jobexec',
                                                    'jobDescription.xml', '-o', 'LogLevel=info']
  assert ret.get('Message', None) is None
  assert ret['OK']
