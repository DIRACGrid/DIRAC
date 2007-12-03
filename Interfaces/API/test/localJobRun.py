########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/test/localJobRun.py,v 1.1 2007/12/03 18:23:11 paterson Exp $
# File  : TestJob.py
# Author: Stuart Paterson
########################################################################

from DIRAC.Interfaces.API.Job                            import Job

import unittest,types,time,sys

#from DIRAC.Interfaces.API.DIRAC                            import *
from DIRAC                                                 import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
import os
#############################################################################

class JobTests:

  def __init__(self):
    #self.__printInfo()
    pass

  def main(self):
    print 'Job Tests starting'
    result  = self.basicTest()

  def basicTest(self):
    j = Job()
    j.setCPUTime(50000)
    j.setExecutable('/Users/stuart/dirac/workspace/DIRAC3/DIRAC/Interfaces/API/test/myPythonScript.py')
    j.setExecutable('/bin/echo hello')
    j.setOwner('paterson')
    j.setType('test')
    j.setName('MyJobName')
    #j.setAncestorDepth(1)
    j.setInputSandbox(['/Users/stuart/dirac/workspace/DIRAC3/DIRAC/Interfaces/API/test/DV.opts','/Users/stuart/dirac/workspace/DIRAC3/DIRAC/Interfaces/API/test/DV2.opts'])
    j.setOutputSandbox(['firstfile.txt','anotherfile.root'])
    j.setInputData(['/lhcb/production/DC04/v2/DST/00000742_00003493_11.dst',
                    '/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst'])
    j.setOutputData(['my.dst','myfile.log'])
    j.setDestination('LCG.CERN.ch')
    j.setPlatform('LCG')
    j.setSystemConfig('slc4_ia32_gcc34')
    j.setSoftwareTags(['VO-lhcb-Brunel-v30r17','VO-lhcb-Boole-v12r10'])
    #print j._toJDL()
    #print j.printObj()
    xml = j._toXML()
    testFile = '/Users/stuart/dirac/workspace/DIRAC3/DIRAC/Interfaces/API/test/jobxml.xml'
    if os.path.exists(testFile):
      os.remove(testFile)
    xmlfile = open(testFile,'w')
    xmlfile.write(xml)
    xmlfile.close()
    print 'Creating code for the workflow'
    print j.createCode()
    j.execute()
    output = shellCall(0,'/Users/stuart/dirac/workspace/DIRAC3/scripts/jobexec %s' %(testFile))

    stdout = output['Value'][1]
    print stdout
    stderr = output['Value'][2]
    print stderr


  #############################################################################
  #############################################################################

class JobRunTestCase(unittest.TestCase):
  """ Base class for the Job test cases
  """
  def test_runJob(self):
    j = JobTests()
    j.main()

if __name__ == '__main__':
  print 'Starting Unit Test for Job'
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobRunTestCase)
  print 'Unit test finished'
 # suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GetSystemInfoTestCase))
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

