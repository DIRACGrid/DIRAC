""" Unit Test of Workflow Modules
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import itertools
import os
import copy
import shutil

from mock import MagicMock as Mock

from DIRAC import gLogger


class ModulesTestCase(unittest.TestCase):
  """ Base class for the Modules test cases
  """

  def setUp(self):

    gLogger.setLevel('ERROR')
#    import sys
#    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.ResourceStatusSystem.Utilities.CS"] = DIRAC.ResourceStatusSystem.test.fake_Logger

    self.jr_mock = Mock()
    self.jr_mock.setApplicationStatus.return_value = {'OK': True, 'Value': ''}
    self.jr_mock.generateRequest.return_value = {'OK': True, 'Value': 'pippo'}
    self.jr_mock.setJobParameter.return_value = {'OK': True, 'Value': 'pippo'}
    self.jr_mock.generateForwardDISET.return_value = {'OK': True, 'Value': 'pippo'}
#    self.jr_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': 'pippo'}

    self.fr_mock = Mock()
    self.fr_mock.getFiles.return_value = {}
    self.fr_mock.setFileStatus.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.commit.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.generateRequest.return_value = {'OK': True, 'Value': ''}

    rc_mock = Mock()
    rc_mock.update.return_value = {'OK': True, 'Value': ''}
    rc_mock.setDISETRequest.return_value = {'OK': True, 'Value': ''}
    rc_mock.isEmpty.return_value = {'OK': True, 'Value': ''}
    rc_mock.toXML.return_value = {'OK': True, 'Value': ''}
    rc_mock.getDigest.return_value = {'OK': True, 'Value': ''}
    rc_mock.__len__.return_value = 1
    self.rc_mock = rc_mock

    ar_mock = Mock()
    ar_mock.commit.return_value = {'OK': True, 'Value': ''}

    self.rm_mock = Mock()
    self.rm_mock.getReplicas.return_value = {'OK': True, 'Value': {'Successful': {'pippo': 'metadataPippo'},
                                                                   'Failed': None}}
    self.rm_mock.getCatalogFileMetadata.return_value = {'OK': True, 'Value': {'Successful': {'pippo': 'metadataPippo'},
                                                                              'Failed': None}}
    self.rm_mock.removeFile.return_value = {'OK': True, 'Value': {'Failed': False}}
    self.rm_mock.putStorageDirectory.return_value = {'OK': True, 'Value': {'Failed': False}}
    self.rm_mock.addCatalogFile.return_value = {'OK': True, 'Value': {'Failed': False}}
    self.rm_mock.putAndRegister.return_value = {'OK': True, 'Value': {'Failed': False}}
    self.rm_mock.getFile.return_value = {'OK': True, 'Value': {'Failed': False}}

    self.jsu_mock = Mock()
    self.jsu_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': ''}

    self.jsu_mock = Mock()
    self.jsu_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': ''}

    request_mock = Mock()
    request_mock.addSubRequest.return_value = {'OK': True, 'Value': ''}
    request_mock.setSubRequestFiles.return_value = {'OK': True, 'Value': ''}
    request_mock.getNumSubRequests.return_value = {'OK': True, 'Value': ''}
    request_mock._getLastOrder.return_value = 1

    self.ft_mock = Mock()
    self.ft_mock.transferAndRegisterFile.return_value = {'OK': True, 'Value': {'uploadedSE': ''}}
    self.ft_mock.transferAndRegisterFileFailover.return_value = {'OK': True, 'Value': {}}

    self.nc_mock = Mock()
    self.nc_mock.sendMail.return_value = {'OK': True, 'Value': ''}

    self.prod_id = 123
    self.prod_job_id = 456
    self.wms_job_id = 0
    self.workflowStatus = {'OK': True}
    self.stepStatus = {'OK': True}

    self.wf_commons = [{'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'eventType': '123456789',
                        'jobType': 'merge',
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'numberOfEvents': '100',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'merge',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'numberOfEvents': '100',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'merge',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'numberOfEvents': '100',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'merge',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'numberOfEvents': '100',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'InputData': '',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'InputData': 'foo;bar',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'InputData': 'foo;bar',
                        'ParametricInputData': '',
                        'appSteps': ['someApp_1']},
                       {'PRODUCTION_ID': str(self.prod_id),
                        'JOB_ID': str(self.prod_job_id),
                        'configName': 'aConfigName',
                        'configVersion': 'aConfigVersion',
                        'outputDataFileMask': '',
                        'jobType': 'reco',
                        'BookkeepingLFNs': 'aa',
                        'ProductionOutputData': 'ProductionOutputData',
                        'JobReport': self.jr_mock,
                        'Request': rc_mock,
                        'AccountingReport': ar_mock,
                        'FileReport': self.fr_mock,
                        'LogFilePath': 'someDir',
                        'LogTargetPath': 'someOtherDir',
                        'runNumber': 'Unknown',
                        'InputData': 'foo;bar',
                        'ParametricInputData': 'pid1;pid2;pid3',
                        'appSteps': ['someApp_1']},
                       ]
    self.step_commons = [{'applicationName': 'someApp',
                          'applicationVersion': 'v1r0',
                          'eventType': '123456789',
                          'applicationLog': 'appLog',
                          'extraPackages': '',
                          'XMLSummary': 'XMLSummaryFile',
                          'numberOfEvents': '100',
                          'BKStepID': '123',
                          'StepProcPass': 'Sim123',
                          'outputFilePrefix': 'pref_',
                          'STEP_INSTANCE_NAME': 'someApp_1',
                          'listoutput': [{'outputDataName': str(self.prod_id) + '_' + str(self.prod_job_id) + '_',
                                          'outputDataSE': 'aaa',
                                          'outputDataType': 'bbb'}]},
                         {'applicationName': 'someApp',
                          'applicationVersion': 'v1r0',
                          'eventType': '123456789',
                          'applicationLog': 'appLog',
                          'extraPackages': '',
                          'XMLSummary': 'XMLSummaryFile',
                          'numberOfEvents': '100',
                          'BKStepID': '123',
                          'StepProcPass': 'Sim123',
                          'outputFilePrefix': 'pref_',
                          'optionsLine': '',
                          'STEP_INSTANCE_NAME': 'someApp_1',
                          'listoutput': [{'outputDataName': str(self.prod_id) + '_' + str(self.prod_job_id) + '_',
                                          'outputDataSE': 'aaa',
                                          'outputDataType': 'bbb'}]},
                         {'applicationName': 'someApp',
                          'applicationVersion': 'v1r0',
                          'eventType': '123456789',
                          'applicationLog': 'appLog',
                          'extraPackages': '',
                          'XMLSummary': 'XMLSummaryFile',
                          'numberOfEvents': '100',
                          'BKStepID': '123',
                          'StepProcPass': 'Sim123',
                          'outputFilePrefix': 'pref_',
                          'extraOptionsLine': 'blaBla',
                          'STEP_INSTANCE_NAME': 'someApp_1',
                          'listoutput': [{'outputDataName': str(self.prod_id) + '_' + str(self.prod_job_id) + '_',
                                          'outputDataSE': 'aaa',
                                          'outputDataType': 'bbb'}]}]
    self.step_number = '321'
    self.step_id = '%s_%s_%s' % (self.prod_id, self.prod_job_id, self.step_number)

    from DIRAC.Workflow.Modules.ModuleBase import ModuleBase
    self.mb = ModuleBase()

    self.mb.rm = self.rm_mock
    self.mb.request = self.rc_mock
    self.mb.jobReport = self.jr_mock
    self.mb.fileReport = self.fr_mock
    self.mb.workflow_commons = self.wf_commons[0]

    from DIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
    self.fr = FailoverRequest()

    self.fr.request = self.rc_mock
    self.fr.jobReport = self.jr_mock
    self.fr.fileReport = self.fr_mock

    from DIRAC.Workflow.Modules.Script import Script
    self.script = Script()

    self.script.request = self.rc_mock
    self.script.jobReport = self.jr_mock
    self.script.fileReport = self.fr_mock

  def tearDown(self):
    for fileProd in ['appLog', 'foo.txt', 'aaa.Bhadron.dst', 'bbb.Calibration.dst', 'bar_2.py', 'foo_1.txt',
                     'ccc.charm.mdst', 'prova.txt', 'foo.txt', 'BAR.txt', 'FooBAR.ext.txt', 'applicationLog.txt',
                     'ErrorLogging_Step1_coredump.log', '123_00000456_request.xml', 'lfn1', 'lfn2',
                     'aaa.bhadron.dst', 'bbb.calibration.dst', 'ProductionOutputData', 'data.py',
                     '00000123_00000456.tar', 'someOtherDir', 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK',
                     ]:
      try:
        os.remove(fileProd)
      except OSError:
        continue

    for directory in ['./job', 'job']:
      try:
        shutil.rmtree(directory)
      except BaseException:
        continue

#############################################################################
# ModuleBase.py
#############################################################################


class ModuleBaseSuccess(ModulesTestCase):

  #################################################

  def test__checkLocalExistance(self):

    self.assertRaises(OSError, self.mb._checkLocalExistance, ['aaa', 'bbb'])

  #################################################

  def test__applyMask(self):

    candidateFiles = {'00012345_00012345_4.dst':
                      {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                       'type': 'dst',
                       'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_2.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_3.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_5.AllStreams.dst':
                      {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                       'type': 'allstreams.dst',
                       'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}}

    fileMasks = (['dst'], 'dst', ['sim'], ['digi'], ['digi', 'sim'], 'allstreams.dst')
    stepMasks = ('', '5', '', ['2'], ['1', '3'], '')

    results = ({'00012345_00012345_4.dst':
                {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                 'type': 'dst',
                 'workflowSE': 'Tier1_MC_M-DST'}
                },
               {},
               {'00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}
                },
               {'00012345_00012345_2.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                },
               {'00012345_00012345_3.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}
                },
               {'00012345_00012345_5.AllStreams.dst':
                {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                 'type': 'allstreams.dst',
                 'workflowSE': 'Tier1_MC_M-DST'}
                }
               )

    for fileMask, result, stepMask in zip(fileMasks, results, stepMasks):
      res = self.mb._applyMask(candidateFiles, fileMask, stepMask)
      self.assertEqual(res, result)

  #################################################

  def test__checkSanity(self):

    candidateFiles = {'00012345_00012345_4.dst':
                      {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_4.dst',
                       'type': 'dst',
                       'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_2.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_3.digi': {'type': 'digi', 'workflowSE': 'Tier1-RDST'},
                      '00012345_00012345_5.AllStreams.dst':
                      {'lfn': '/lhcb/MC/2010/DST/00012345/0001/00012345_00012345_5.AllStreams.dst',
                       'type': 'DST',
                       'workflowSE': 'Tier1_MC_M-DST'},
                      '00012345_00012345_1.sim': {'type': 'sim', 'workflowSE': 'Tier1-RDST'}}

    self.assertRaises(ValueError, self.mb._checkSanity, candidateFiles)

  #################################################

  def test_getCandidateFiles(self):
    # this needs to avoid the "checkLocalExistance"

    open('foo_1.txt', 'w').close()
    open('bar_2.py', 'w').close()

    outputList = [{'outputDataType': 'txt', 'outputDataSE': 'Tier1-RDST', 'outputDataName': 'foo_1.txt'},
                  {'outputDataType': 'py', 'outputDataSE': 'Tier1-RDST', 'outputDataName': 'bar_2.py'}]
    outputLFNs = ['/lhcb/MC/2010/DST/00012345/0001/foo_1.txt', '/lhcb/MC/2010/DST/00012345/0001/bar_2.py']
    fileMask = 'txt'
    stepMask = ''
    result = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                            'type': outputList[0]['outputDataType'],
                            'workflowSE': outputList[0]['outputDataSE']}}

    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)
    self.assertEqual(res, result)

    fileMask = ['txt', 'py']
    stepMask = None
    result = {'foo_1.txt': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/foo_1.txt',
                            'type': outputList[0]['outputDataType'],
                            'workflowSE': outputList[0]['outputDataSE']},
              'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']},
              }
    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)
    self.assertEqual(res, result)

    fileMask = ['aa']
    stepMask = None
    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)
    result = {}
    self.assertEqual(res, result)

    fileMask = ''
    stepMask = '2'
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']}}

    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)

    self.assertEqual(res, result)

    fileMask = ''
    stepMask = 2
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']}}

    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)

    self.assertEqual(res, result)

    fileMask = ''
    stepMask = ['2', '3']
    result = {'bar_2.py': {'lfn': '/lhcb/MC/2010/DST/00012345/0001/bar_2.py',
                           'type': outputList[1]['outputDataType'],
                           'workflowSE': outputList[1]['outputDataSE']}}

    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)

    self.assertEqual(res, result)

    fileMask = ''
    stepMask = ['3']
    result = {}

    res = self.mb.getCandidateFiles(outputList, outputLFNs, fileMask, stepMask)

    self.assertEqual(res, result)

  def test__enableModule(self):

    self.mb.production_id = self.prod_id
    self.mb.prod_job_id = self.prod_job_id
    self.mb.jobID = self.wms_job_id
    self.mb.workflowStatus = self.workflowStatus
    self.mb.stepStatus = self.stepStatus
    self.mb.workflow_commons = self.wf_commons[0]  # APS: this is needed
    self.mb.step_commons = self.step_commons[0]
    self.mb.step_number = self.step_number
    self.mb.step_id = self.step_id
    self.mb.execute()
    self.assertFalse(self.mb._enableModule())

    self.mb.jobID = 1
    self.mb.execute()
    self.assertTrue(self.mb._enableModule())

  def test__determineStepInputData(self):

    self.mb.stepName = 'DaVinci_2'

    inputData = 'previousStep'
    self.mb.appSteps = ['Brunel_1', 'DaVinci_2']
    self.mb.workflow_commons = {'outputList': [{'stepName': 'Brunel_1',
                                                'outputDataType': 'brunelhist',
                                                'outputBKType': 'BRUNELHIST',
                                                'outputDataSE': 'CERN-HIST',
                                                'outputDataName': 'Brunel_00012345_00006789_1_Hist.root'},
                                               {'stepName': 'Brunel_1',
                                                'outputDataType': 'sdst',
                                                'outputBKType': 'SDST',
                                                'outputDataSE': 'Tier1-BUFFER',
                                                'outputDataName': '00012345_00006789_1.sdst'}
                                               ]
                                }
    self.mb.inputDataType = 'SDST'

    first = self.mb._determineStepInputData(inputData)
    second = ['00012345_00006789_1.sdst']
    self.assertEqual(first, second)

    inputData = 'previousStep'
    self.mb.appSteps = ['Brunel_1', 'DaVinci_2']
    self.mb.workflow_commons['outputList'] = [{'stepName': 'Brunel_1',
                                               'outputDataType': 'brunelhist',
                                               'outputBKType': 'BRUNELHIST',
                                               'outputDataSE': 'CERN-HIST',
                                               'outputDataName': 'Brunel_00012345_00006789_1_Hist.root'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataSE': 'Tier1-BUFFER',
                                               'outputDataName': 'some.sdst'},
                                              {'stepName': 'Brunel_1',
                                               'outputDataType': 'sdst',
                                               'outputBKType': 'SDST',
                                               'outputDataSE': 'Tier1-BUFFER',
                                               'outputDataName': '00012345_00006789_1.sdst'}
                                              ]
    self.mb.inputDataType = 'SDST'
    first = self.mb._determineStepInputData(inputData)
    second = ['some.sdst', '00012345_00006789_1.sdst']
    self.assertEqual(first, second)

    inputData = 'LFN:123.raw'
    first = self.mb._determineStepInputData(inputData)
    second = ['123.raw']
    self.assertEqual(first, second)


#############################################################################
# FailoverRequest.py
#############################################################################

class FailoverRequestSuccess(ModulesTestCase):

  #################################################

  def test_execute(self):

    self.fr.jobType = 'merge'
    self.fr.stepInputData = ['foo', 'bar']

    self.fr.production_id = self.prod_id
    self.fr.prod_job_id = self.prod_job_id
    self.fr.jobID = self.wms_job_id
    self.fr.workflowStatus = self.workflowStatus
    self.fr.stepStatus = self.stepStatus
    self.fr.workflow_commons = self.wf_commons
    self.fr.step_commons = self.step_commons[0]
    self.fr.step_number = self.step_number
    self.fr.step_id = self.step_id

    # no errors, no input data
    for wf_commons in copy.deepcopy(self.wf_commons):
      for step_commons in self.step_commons:
        self.fr.workflow_commons = wf_commons
        self.fr.step_commons = step_commons
        res = self.fr.execute()
        self.assertTrue(res['OK'])

#############################################################################
# Scripy.py
#############################################################################


class ScriptSuccess(ModulesTestCase):

  #################################################

  def test_execute(self):

    self.script.jobType = 'merge'
    self.script.stepInputData = ['foo', 'bar']

    self.script.production_id = self.prod_id
    self.script.prod_job_id = self.prod_job_id
    self.script.jobID = self.wms_job_id
    self.script.workflowStatus = self.workflowStatus
    self.script.stepStatus = self.stepStatus
    self.script.workflow_commons = self.wf_commons
    self.script.step_commons = self.step_commons[0]
    self.script.step_number = self.step_number
    self.script.step_id = self.step_id
    self.script.executable = 'ls'
    self.script.applicationLog = 'applicationLog.txt'

    # no errors, no input data
    for wf_commons in copy.deepcopy(self.wf_commons):
      for step_commons in self.step_commons:
        self.script.workflow_commons = wf_commons
        self.script.step_commons = step_commons
        self.script._setCommand()
        self.script._executeCommand()


class ScriptFailure(ModulesTestCase):

  #################################################

  def test_execute(self):

    self.script.jobType = 'merge'
    self.script.stepInputData = ['foo', 'bar']

    self.script.production_id = self.prod_id
    self.script.prod_job_id = self.prod_job_id
    self.script.jobID = self.wms_job_id
    self.script.workflowStatus = self.workflowStatus
    self.script.stepStatus = self.stepStatus
    self.script.workflow_commons = self.wf_commons
    self.script.step_commons = self.step_commons[0]
    self.script.step_number = self.step_number
    self.script.step_id = self.step_id

    # no errors, no input data
    for wf_commons in copy.deepcopy(self.wf_commons):
      for step_commons in self.step_commons:
        self.script.workflow_commons = wf_commons
        self.script.step_commons = step_commons
        res = self.script.execute()
        self.assertFalse(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ModulesTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ModuleBaseSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FailoverRequestSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ScriptSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ScriptFailure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
