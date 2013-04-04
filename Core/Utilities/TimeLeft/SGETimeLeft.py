########################################################################
# $Id$
########################################################################

""" The SGE TimeLeft utility interrogates the SGE batch system for the
    current CPU consumed, as well as its limit.
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.TimeLeft.TimeLeft import runCommand

__RCSID__ = "$Id$"

import os, re, time, socket

class SGETimeLeft:
  """
   This is the SGE plugin of the TimeLeft Utility
  """

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'SGETimeLeft' )
    self.jobID = None
    if os.environ.has_key( 'JOB_ID' ):
      self.jobID = os.environ['JOB_ID']
    self.queue = None
    if os.environ.has_key( 'QUEUE' ):
      self.queue = os.environ['QUEUE']
    if os.environ.has_key( 'SGE_BINARY_PATH' ):
      pbsPath = os.environ['SGE_BINARY_PATH']
      os.environ['PATH'] = os.environ['PATH'] + ':' + pbsPath

    self.cpuLimit = None
    self.wallClockLimit = None
    self.log.verbose( 'JOB_ID=%s, QUEUE=%s' % ( self.jobID, self.queue ) )
    self.startTime = time.time()

  #############################################################################
  def getResourceUsage( self ):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    cmd = 'qstat -f -j %s' % ( self.jobID )
    result = runCommand( cmd )
    if not result['OK']:
      return result
    example = """ Example of output from qstat -f -j $JOB_ID
==============================================================
job_number:                 620685
exec_file:                  job_scripts/620685
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
hard resource_list:         os=sl5,s_cpu=165600,s_vmem=5120M,s_fsize=51200M,cvmfs=1,dcache=1
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
usage    1:                 cpu=00:00:07, mem=0.03044 GBs, io=0.19846, vmem=288.609M, maxvmem=288.609M
scheduling info:            (Collecting of scheduler job information is turned off)
    """


    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    lines = result['Value'].split( '\n' )
    for line in lines:
      if re.search( 'usage.*cpu.*', line ):
        match = re.search( 'cpu=([\d,:]*),', line )
        if match:
          cpuList = match.groups()[0].split( ':' )
        try:
          newcpu = 0.
          if len( cpuList ) == 3:
            newcpu = ( float( cpuList[0] ) * 60 + float( cpuList[1] ) ) * 60 + float( cpuList[2] )
          elif len( cpuList ) == 4:
            newcpu = ( ( float( cpuList[0] ) * 24 + float( cpuList[1] ) ) * 60 + float( cpuList[2] ) ) * 60 + float( cpuList[3] )              
          if not cpu or newcpu > cpu:
            cpu = newcpu
        except ValueError:
          self.log.warn( 'Problem parsing "%s" for CPU consumed' % line )
      if re.search( 'hard resource_list.*cpu.*', line ):
        match = re.search( '_cpu=(\d*)', line )
        if match:
          cpuLimit = float( match.groups()[0] )
        match = re.search( '_rt=(\d*)', line )
        if match:
          wallClockLimit = float( match.groups()[0] )

    # Some SGE batch systems apply CPU scaling factor to the CPU consumption figures
    if cpu:
      factor = self.__getCPUScalingFactor()
      if factor:
        cpu = cpu/factor

    consumed = {'CPU':cpu, 'CPULimit':cpuLimit, 'WallClock':wallClock, 'WallClockLimit':wallClockLimit}
    self.log.debug( consumed )
    failed = False
    for key, val in consumed.items():
      if val == None:
        failed = True
        self.log.warn( 'Could not determine %s' % key )

    if not failed:
      return S_OK( consumed )

    if cpuLimit or wallClockLimit:
      # We have got a partial result from SGE
      if not cpuLimit:
        consumed['CPULimit'] = wallClockLimit
      if not wallClockLimit:
        consumed['WallClockLimit'] = cpuLimit
      if not cpu:
        consumed['CPU'] = time.time() - self.startTime
      if not wallClock:
        consumed['WallClock'] = time.time() - self.startTime
      self.log.debug( "TimeLeft counters restored: " + str( consumed ) )
      return S_OK( consumed )
    else:
      self.log.info( 'Could not determine some parameters, this is the stdout from the batch system call\n%s' % ( result['Value'] ) )
      retVal = S_ERROR( 'Could not determine some parameters' )
      retVal['Value'] = consumed
      return retVal

  def __getCPUScalingFactor(self):

    host = socket.getfqdn()
    cmd = 'qconf -se %s' % host
    result = runCommand( cmd )
    if not result['OK']:
      return None
    lines = result['Value'].split( '\n' )
    for line in lines:
      if re.search( 'usage_scaling', line ):
        match = re.search('cpu=([\d,\.]*),',line)
        if match:
          return float( match.groups()[0] )
    return None

if __name__ == '__main__':
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()
  print SGETimeLeft().getResourceUsage()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

