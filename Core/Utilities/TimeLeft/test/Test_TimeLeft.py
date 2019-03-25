""" Test class for TimeLeft utility

    (Partially) tested here are SGE and LSF, PBS is TO-DO
"""

#FIXME: remove use of importlib, use @mock.patch decorator instead

# imports
from __future__ import print_function
import unittest, importlib
from mock import MagicMock, patch

from DIRAC import gLogger, S_OK

# sut
from DIRAC.Core.Utilities.TimeLeft.TimeLeft import TimeLeft, enoughTimeLeft

SGE_ReturnValue = """==============================================================
job_number:                 12345
exec_file:                  job_scripts/12345
submission_time:            Wed Apr 11 09:36:41 2012
owner:                      lhcb049
uid:                        18416
group:                      lhcb
gid:                        155
sge_o_home:                 /home/lhcb049
sge_o_log_name:             lhcb049
sge_o_path:                 /opt/sge/bin/lx24-amd64:/usr/bin:/bin
sge_o_shell:                /bin/sh
sge_o_workdir:              /var/glite/tmp
sge_o_host:                 cccreamceli05
account:                    GRID=EGI SITE=IN2P3-CC TIER=tier1 VO=lhcb ROLEVOMS=&2Flhcb&2FRole=pilot&2FCapability=NULL DN=&2FDC=ch&2FDC=cern&2FOU=Organic&20Units&2FOU=Users&2FCN=romanov&2FCN=427293&2FCN=Vladimir&20Romanovskiy&2FCN=proxy&2FCN=proxy&2FCN=proxy&2FCN=proxy
merge:                      y
hard resource_list:         os=sl5,s_cpu=1000,s_vmem=5120M,s_fsize=51200M,cvmfs=1,dcache=1
mail_list:                  lhcb049@cccreamceli05.in2p3.fr
notify:                     FALSE
job_name:                   cccreamceli05_crm05_749996134
stdout_path_list:           NONE:NONE:/dev/null
jobshare:                   0
hard_queue_list:            huge
restart:                    n
shell_list:                 NONE:/bin/bash
env_list:                   SITE_NAME=IN2P3-CC,MANPATH=/opt/sge/man:/usr/share/man:/usr/local/man:/usr/local/share/man,HOSTNAME=cccreamceli05,SHELL=/bin/sh,TERM=vanilla,HISTSIZE=1000,SGE_CELL=ccin2p3,USER=lhcb049,LD_LIBRARY_PATH=/usr/lib64:,LS_COLORS=no=00:fi=00:di=01;34:ln=01;36:pi=40;33:so=01;35:bd=40;33;01:cd=40;33;01:or=01;05;37;41:mi=01;05;37;41:ex=01;32:*.cmd=01;32:*.exe=01;32:*.com=01;32:*.btm=01;32:*.bat=01;32:*.sh=01;32:*.csh=01;32:*.tar=01;31:*.tgz=01;31:*.arj=01;31:*.taz=01;31:*.lzh=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.gz=01;31:*.bz2=01;31:*.bz=01;31:*.tz=01;31:*.rpm=01;31:*.cpio=01;31:*.jpg=01;35:*.gif=01;35:*.bmp=01;35:*.xbm=01;35:*.xpm=01;35:*.png=01;35:*.tif=01;35:,SUDO_USER=tomcat,SUDO_UID=91,USERNAME=lhcb049,PATH=/opt/sge/bin/lx24-amd64:/usr/bin:/bin,MAIL=/var/spool/mail/tomcat,PWD=/var/glite/tmp,INPUTRC=/etc/inputrc,SGE_EXECD_PORT=10501,SGE_QMASTER_PORT=10500,SGE_ROOT=/opt/sge,SHLVL=1,SUDO_COMMAND=/opt/glite/bin/sge_submit.sh -x /var/glite/cream_sandbox/lhcb/_DC_ch_DC_cern_OU_Organic_Units_OU_Users_CN_romanov_CN_427293_CN_Vladimir_Romanovskiy_lhcb_Role_pilot_Capability_NULL_lhcb049/proxy/354BFF4A_EAD9_3B10_FBE7_D9FFB765662A11488451642439 -u /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=romanov/CN=427293/CN=Vladimir Romanovskiy -r no -c /var/glite/cream_sandbox/lhcb/_DC_ch_DC_cern_OU_Organic_Units_OU_Users_CN_romanov_CN_427293_CN_Vladimir_Romanovskiy_lhcb_Role_pilot_Capability_NULL_lhcb049/74/CREAM749996134/CREAM749996134_jobWrapper.sh -T /tmp -C /tmp/ce-req-file-1334129801228226 -o /var/glite/cream_sandbox/lhcb/_DC_ch_DC_cern_OU_Organic_Units_OU_Users_CN_romanov_CN_427293_CN_Vladimir_Romanovskiy_lhcb_Role_pilot_Capability_NULL_lhcb049/74/CREAM749996134/StandardOutput -e /var/glite/cream_sandbox/lhcb/_DC_ch_DC_cern_OU_Organic_Units_OU_Users_CN_romanov_CN_427293_CN_Vladimir_Romanovskiy_lhcb_Role_pilot_Capability_NULL_lhcb049/74/CREAM749996134/StandardError -q verylong -j crm05_749996134,HOME=/home/lhcb049,LOGNAME=lhcb049,SGE_CLUSTER_NAME=prod,SUDO_GID=91,DISPLAY=localhost:10.0,XAUTHORITY=/tmp/ssh-oosv2628/cookies,_=/opt/sge/bin/lx24-amd64/qsub
script_file:                /tmp/crm05_749996134
project:                    P_lhcb_pilot
usage    1:                 cpu=00:01:00, mem=0.03044 GBs, io=0.19846, vmem=288.609M, maxvmem=288.609M
scheduling info:            (Collecting of scheduler job information is turned off)"""

PBS_ReturnValue = """bla"""

LSF_ReturnValue = """JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  PROJ_NAME CPU_USED MEM SWAP PIDS START_TIME FINISH_TIME
12345 lhbplt01 RUN   grid_lhcb  ce407.cern.ch p01001532668097 cream_220615831 10/12-20:51:42 default    00:00:60.00 626732 4071380 13723,13848,13852,14054,14055,14112,14117,14247,14248,14251,14253,14256,28412,20315,20316,23628,25459,25468,25469,14249 10/12-20:52:00 -"""

MJF_ReturnValue = "0"


class TimeLeftTestCase( unittest.TestCase ):
  """ Base class for the test cases
  """
  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.tl = None

  def tearDown( self ):
    pass


class TimeLeftSuccess( TimeLeftTestCase ):

  def test_enoughTimeLeft(self):
    res = enoughTimeLeft(cpu=100., cpuLimit=1000., wallClock=50., wallClockLimit=80., cpuMargin=3, wallClockMargin=10)
    self.assertTrue(res)
    print('\n')
    res = enoughTimeLeft(cpu=900., cpuLimit=1000., wallClock=0., wallClockLimit=80., cpuMargin=3, wallClockMargin=10)
    self.assertTrue(res)
    print('\n')
    res = enoughTimeLeft(cpu=990., cpuLimit=1000., wallClock=0., wallClockLimit=80., cpuMargin=3, wallClockMargin=10)
    self.assertFalse(res)
    print('\n')
    res = enoughTimeLeft(cpu=100., cpuLimit=1000., wallClock=90., wallClockLimit=80., cpuMargin=3, wallClockMargin=10)
    self.assertFalse(res)
    print('\n')
    res = enoughTimeLeft(cpu=100., cpuLimit=1000., wallClock=50., wallClockLimit=80., cpuMargin=0, wallClockMargin=10)
    self.assertTrue(res)
    print('\n')
    res = enoughTimeLeft(cpu=100., cpuLimit=1000., wallClock=50., wallClockLimit=80., cpuMargin=0, wallClockMargin=10)
    self.assertTrue(res)

  def test_getScaledCPU( self ):
    tl = TimeLeft()
    res = tl.getScaledCPU()
    self.assertEqual( res, 0 )

    tl.scaleFactor = 5.0
    tl.normFactor = 5.0

    for batch, retValue in [( 'LSF', LSF_ReturnValue )]:
      self.tl = importlib.import_module( "DIRAC.Core.Utilities.TimeLeft.TimeLeft" )
      rcMock = MagicMock()
      rcMock.return_value = S_OK( retValue )
      self.tl.runCommand = rcMock

      batchSystemName = '%sTimeLeft' % batch
      batchPlugin = __import__( 'DIRAC.Core.Utilities.TimeLeft.%s' % #pylint: disable=unused-variable
                                batchSystemName, globals(), locals(), [batchSystemName] )
      batchStr = 'batchPlugin.%s()' % ( batchSystemName )
      tl.batchPlugin = eval( batchStr )
      res = tl.getScaledCPU()
      self.assertEqual( res, 0.0 )

    for batch, retValue in [( 'SGE', SGE_ReturnValue )]:
      self.tl = importlib.import_module( "DIRAC.Core.Utilities.TimeLeft.TimeLeft" )
      rcMock = MagicMock()
      rcMock.return_value = S_OK( retValue )
      self.tl.runCommand = rcMock

      batchSystemName = '%sTimeLeft' % batch
      batchPlugin = __import__( 'DIRAC.Core.Utilities.TimeLeft.%s' % #pylint: disable=unused-variable
                                batchSystemName, globals(), locals(), [batchSystemName] )
      batchStr = 'batchPlugin.%s()' % ( batchSystemName )
      tl.batchPlugin = eval( batchStr )
      res = tl.getScaledCPU()
      self.assertEqual( res, 300.0 )

      for batch, retValue in [( 'MJF', MJF_ReturnValue )]:
        self.tl = importlib.import_module( "DIRAC.Core.Utilities.TimeLeft.TimeLeft" )
        rcMock = MagicMock()
        rcMock.return_value = S_OK( retValue )
        self.tl.runCommand = rcMock

        batchSystemName = '%sTimeLeft' % batch
        batchPlugin = __import__( 'DIRAC.Core.Utilities.TimeLeft.%s' % #pylint: disable=unused-variable
                                  batchSystemName, globals(), locals(), [batchSystemName] )
        batchStr = 'batchPlugin.%s()' % ( batchSystemName )
        tl.batchPlugin = eval( batchStr )
        res = tl.getScaledCPU()
        self.assertEqual( res, 0.0 )


  def test_getTimeLeft( self ):
#     for batch, retValue in [( 'LSF', LSF_ReturnValue ), ( 'SGE', SGE_ReturnValue )]:

    for batch, retValue in [( 'LSF', LSF_ReturnValue )]:
      self.tl = importlib.import_module( "DIRAC.Core.Utilities.TimeLeft.TimeLeft" )
      rcMock = MagicMock()
      rcMock.return_value = S_OK( retValue )
      self.tl.runCommand = rcMock

      timeMock = MagicMock()

      tl = TimeLeft()
#      res = tl.getTimeLeft()
#      self.assertEqual( res['OK'], True )

      batchSystemName = '%sTimeLeft' % batch
      batchPlugin = __import__( 'DIRAC.Core.Utilities.TimeLeft.%s' %
                                batchSystemName, globals(), locals(), [batchSystemName] )
      batchStr = 'batchPlugin.%s()' % ( batchSystemName )
      tl.batchPlugin = eval( batchStr )

      tl.scaleFactor = 10.0
      tl.normFactor = 10.0
      tl.batchPlugin.bin = '/usr/bin'
      tl.batchPlugin.hostNorm = 10.0
      tl.batchPlugin.cpuLimit = 1000
      tl.batchPlugin.wallClockLimit = 1000

      with patch( "DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft.runCommand", new=rcMock ):
        with patch( "DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft.time", new=timeMock ):
          res = tl.getTimeLeft()
          self.assertEqual( res['OK'], True, res.get('Message', '') )

    for batch, retValue in [( 'SGE', SGE_ReturnValue )]:
      self.tl = importlib.import_module( "DIRAC.Core.Utilities.TimeLeft.TimeLeft" )
      rcMock = MagicMock()
      rcMock.return_value = S_OK( retValue )
      self.tl.runCommand = rcMock

      tl = TimeLeft()
#       res = tl.getTimeLeft()
#       self.assertFalse( res['OK'] )

      batchSystemName = '%sTimeLeft' % batch
      batchPlugin = __import__( 'DIRAC.Core.Utilities.TimeLeft.%s' %
                                batchSystemName, globals(), locals(), [batchSystemName] )
      batchStr = 'batchPlugin.%s()' % ( batchSystemName )
      tl.batchPlugin = eval( batchStr )

      tl.scaleFactor = 10.0
      tl.normFactor = 10.0
      tl.batchPlugin.bin = '/usr/bin'
      tl.batchPlugin.hostNorm = 10.0
      tl.batchPlugin.cpuLimit = 1000
      tl.batchPlugin.wallClockLimit = 1000

      res = tl.getTimeLeft()
      self.assertTrue(res['OK'])
      self.assertEqual( res['Value'], 9400.0 )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TimeLeftTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TimeLeftSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
