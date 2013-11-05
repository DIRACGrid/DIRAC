########################################################################
# $Id$
# File :   ProcessMonitor.py
# Author : Stuart Paterson
########################################################################

""" The Process Monitor utility allows to calculate cumulative CPU time for a given PID
    and it's process group.  This is only implemented for linux / proc file systems
    but could feasibly be extended in the future.
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"

import os, re, platform

class ProcessMonitor:

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'ProcessMonitor' )
    self.osType = platform.uname()

  #############################################################################
  def getCPUConsumed( self, pid ):
    """Returns the CPU consumed for supported platforms when supplied a PID.
    """
    currentOS = self.__checkCurrentOS()
    if currentOS.lower() == 'linux':
      cpuResult = self.getCPUConsumedLinux( pid )
      return cpuResult
    else:
      self.log.warn( 'Platform %s is not supported' % ( currentOS ) )
      return S_ERROR( 'Unsupported platform' )

  #############################################################################
  def getCPUConsumedLinux( self, pid ):
    """Returns the CPU consumed given a PID assuming a proc file system exists.
    """
    pid = str( pid )
    masterProcPath = '/proc/%s/stat' % ( pid )
    if not os.path.exists( masterProcPath ):
      return S_ERROR( 'Process %s does not exist' % ( pid ) )

    #Get the current process list
    pidListResult = self.__getProcListLinux()
    if not pidListResult['OK']:
      return pidListResult

    pidList = pidListResult['Value']
    #Now recursively add all child process CPU contributions
    currentCPU = self.__getChildCPUConsumedLinux( pid, pidList )

    self.log.verbose( 'Final CPU estimate is %s' % currentCPU )
    return S_OK( currentCPU )

  #############################################################################
  def __getProcListLinux( self ):
    """Gets list of process IDs from /proc/*.
    """
    result = shellCall( 10, 'ls -d /proc/[0-9]*' )

    if not result['OK']:
      if not 'Value' in result:
        return result
    procList = result['Value'][1].replace( '/proc/', '' ).split( '\n' )

    return S_OK( procList )

  #############################################################################
  def __getChildCPUConsumedLinux( self, pid, pidList, infoDict = None ):
    """Adds contributions to CPU total from child processes recursively.
    """
    childCPU = 0
    if not infoDict:
      infoDict = {}
      for pidCheck in pidList:
        info = self.__getProcInfoLinux( pidCheck )
        if info['OK']:
          infoDict[pidCheck] = info['Value']

    procGroup = self.__getProcGroupLinux( pid )
    if not procGroup['OK']:
      return procGroup

    procGroup = procGroup['Value'].strip()

    for pidCheck, info in infoDict.items():
      if pidCheck in infoDict and info[3] == pid:
        contribution = float( info[13] ) / 100 + float( info[14] ) / 100 + float( info[15] ) / 100 + float( info[16] ) / 100
        childCPU += contribution
        self.log.debug( 'Added %s to CPU total (now %s) from child PID %s %s' % ( contribution, childCPU, info[0], info[1] ) )
        del infoDict[pidCheck]
        childCPU += self.__getChildCPUConsumedLinux( pidCheck, pidList, infoDict )


    #Next add any contributions from orphan processes in same process group
    for pidCheck, info in infoDict.items():
      if pidCheck in infoDict and info[3] == 1 and info[4] == procGroup:
        contribution = float( info[13] ) / 100 + float( info[14] ) / 100 + float( info[15] ) / 100 + float( info[16] ) / 100
        childCPU += contribution
        self.log.debug( 'Added %s to CPU total (now %s) from orphan PID %s %s' % ( contribution, childCPU, info[0], info[1] ) )
        del infoDict[pidCheck]

    #Finally add the parent itself
    if pid in infoDict:
      info = infoDict[pid]
      contribution = float( info[13] ) / 100 + float( info[14] ) / 100 + float( info[15] ) / 100 + float( info[16] ) / 100
      childCPU += contribution
      self.log.debug( 'Added %s to CPU total (now %s) from PID %s %s' % ( contribution, childCPU, info[0], info[1] ) )
      del infoDict[pid]

      # Some debug printout if 0 CPU is determined
      if childCPU == 0:
        self.log.error( 'Consumed CPU is found to be 0. Contributing processes:' )
        for pidCheck in pidList:
          if pidCheck not in infoDict:
            info = self.__getProcInfoLinux( pidCheck )
            if info['OK']:
              self.log.error( '  PID:', info['Value'] )


    return childCPU


  #############################################################################
  def __getProcInfoLinux( self, pid ):
    """Attempts to read /proc/PID/stat and returns list of items if ok.
       /proc/[pid]/stat
              Status information about the process.  This is used by ps(1).
              It is defined in /usr/src/linux/fs/proc/array.c.

              The fields, in order, with their proper scanf(3) format
              specifiers, are:

              pid %d      (1) The process ID.

              comm %s     (2) The filename of the executable, in
                          parentheses.  This is visible whether or not the
                          executable is swapped out.

              state %c    (3) One character from the string "RSDZTW" where R
                          is running, S is sleeping in an interruptible
                          wait, D is waiting in uninterruptible disk sleep,
                          Z is zombie, T is traced or stopped (on a signal),
                          and W is paging.

              ppid %d     (4) The PID of the parent.

              pgrp %d     (5) The process group ID of the process.

              session %d  (6) The session ID of the process.

              tty_nr %d   (7) The controlling terminal of the process.  (The
                          minor device number is contained in the
                          combination of bits 31 to 20 and 7 to 0; the major
                          device number is in bits 15 to 8.)

              tpgid %d    (8) The ID of the foreground process group of the
                          controlling terminal of the process.

              flags %u (%lu before Linux 2.6.22)
                          (9) The kernel flags word of the process.  For bit
                          meanings, see the PF_* defines in the Linux kernel
                          source file include/linux/sched.h.  Details depend
                          on the kernel version.

              minflt %lu  (10) The number of minor faults the process has
                          made which have not required loading a memory page
                          from disk.

              cminflt %lu (11) The number of minor faults that the process's
                          waited-for children have made.

              majflt %lu  (12) The number of major faults the process has
                          made which have required loading a memory page
                          from disk.

              cmajflt %lu (13) The number of major faults that the process's
                          waited-for children have made.

              utime %lu   (14) Amount of time that this process has been
                          scheduled in user mode, measured in clock ticks
                          (divide by sysconf(_SC_CLK_TCK)).  This includes
                          guest time, guest_time (time spent running a
                          virtual CPU, see below), so that applications that
                          are not aware of the guest time field do not lose
                          that time from their calculations.

              stime %lu   (15) Amount of time that this process has been
                          scheduled in kernel mode, measured in clock ticks
                          (divide by sysconf(_SC_CLK_TCK)).

              cutime %ld  (16) Amount of time that this process's waited-for
                          children have been scheduled in user mode,
                          measured in clock ticks (divide by
                          sysconf(_SC_CLK_TCK)).  (See also times(2).)  This
                          includes guest time, cguest_time (time spent
                          running a virtual CPU, see below).

              cstime %ld  (17) Amount of time that this process's waited-for
                          children have been scheduled in kernel mode,
                          measured in clock ticks (divide by
                          sysconf(_SC_CLK_TCK)).

              priority %ld
                          (18) (Explanation for Linux 2.6) For processes
                          running a real-time scheduling policy (policy
                          below; see sched_setscheduler(2)), this is the
                          negated scheduling priority, minus one; that is, a
                          number in the range -2 to -100, corresponding to
                          real-time priorities 1 to 99.  For processes
                          running under a non-real-time scheduling policy,
                          this is the raw nice value (setpriority(2)) as
                          represented in the kernel.  The kernel stores nice
                          values as numbers in the range 0 (high) to 39
                          (low), corresponding to the user-visible nice
                          range of -20 to 19.

                          Before Linux 2.6, this was a scaled value based on
                          the scheduler weighting given to this process.

              nice %ld    (19) The nice value (see setpriority(2)), a value
                          in the range 19 (low priority) to -20 (high
                          priority).

              num_threads %ld
                          (20) Number of threads in this process (since
                          Linux 2.6).  Before kernel 2.6, this field was
                          hard coded to 0 as a placeholder for an earlier
                          removed field.

              itrealvalue %ld
                          (21) The time in jiffies before the next SIGALRM
                          is sent to the process due to an interval timer.
                          Since kernel 2.6.17, this field is no longer
                          maintained, and is hard coded as 0.

              starttime %llu (was %lu before Linux 2.6)
                          (22) The time the process started after system
                          boot.  In kernels before Linux 2.6, this value was
                          expressed in jiffies.  Since Linux 2.6, the value
                          is expressed in clock ticks (divide by
                          sysconf(_SC_CLK_TCK)).

              vsize %lu   (23) Virtual memory size in bytes.

              rss %ld     (24) Resident Set Size: number of pages the
                          process has in real memory.  This is just the
                          pages which count toward text, data, or stack
                          space.  This does not include pages which have not
                          been demand-loaded in, or which are swapped out.

              rsslim %lu  (25) Current soft limit in bytes on the rss of the
                          process; see the description of RLIMIT_RSS in
                          getrlimit(2).

              startcode %lu
                          (26) The address above which program text can run.

              endcode %lu (27) The address below which program text can run.

              startstack %lu
                          (28) The address of the start (i.e., bottom) of
                          the stack.

              kstkesp %lu (29) The current value of ESP (stack pointer), as
                          found in the kernel stack page for the process.

              kstkeip %lu (30) The current EIP (instruction pointer).

              signal %lu  (31) The bitmap of pending signals, displayed as a
                          decimal number.  Obsolete, because it does not
                          provide information on real-time signals; use
                          /proc/[pid]/status instead.

              blocked %lu (32) The bitmap of blocked signals, displayed as a
                          decimal number.  Obsolete, because it does not
                          provide information on real-time signals; use
                          /proc/[pid]/status instead.

              sigignore %lu
                          (33) The bitmap of ignored signals, displayed as a
                          decimal number.  Obsolete, because it does not
                          provide information on real-time signals; use
                          /proc/[pid]/status instead.

              sigcatch %lu
                          (34) The bitmap of caught signals, displayed as a
                          decimal number.  Obsolete, because it does not
                          provide information on real-time signals; use
                          /proc/[pid]/status instead.

              wchan %lu   (35) This is the "channel" in which the process is
                          waiting.  It is the address of a system call, and
                          can be looked up in a namelist if you need a
                          textual name.  (If you have an up-to-date
                          /etc/psdatabase, then try ps -l to see the WCHAN
                          field in action.)

              nswap %lu   (36) Number of pages swapped (not maintained).

              cnswap %lu  (37) Cumulative nswap for child processes (not
                          maintained).

              exit_signal %d (since Linux 2.1.22)
                          (38) Signal to be sent to parent when we die.

              processor %d (since Linux 2.2.8)
                          (39) CPU number last executed on.

              rt_priority %u (since Linux 2.5.19; was %lu before Linux
              2.6.22)
                          (40) Real-time scheduling priority, a number in
                          the range 1 to 99 for processes scheduled under a
                          real-time policy, or 0, for non-real-time
                          processes (see sched_setscheduler(2)).

              policy %u (since Linux 2.5.19; was %lu before Linux 2.6.22)
                          (41) Scheduling policy (see
                          sched_setscheduler(2)).  Decode using the SCHED_*
                          constants in linux/sched.h.

              delayacct_blkio_ticks %llu (since Linux 2.6.18)
                          (42) Aggregated block I/O delays, measured in
                          clock ticks (centiseconds).

              guest_time %lu (since Linux 2.6.24)
                          (43) Guest time of the process (time spent running
                          a virtual CPU for a guest operating system),
                          measured in clock ticks (divide by
                          sysconf(_SC_CLK_TCK)).

              cguest_time %ld (since Linux 2.6.24)
                          (44) Guest time of the process's children,
                          measured in clock ticks (divide by
                          sysconf(_SC_CLK_TCK)).
    """
    procPath = '/proc/%s/stat' % ( pid )
    try:
      fopen = open( procPath, 'r' )
      procStat = fopen.readline()
      fopen.close()
    except Exception:
      return S_ERROR( 'Not able to check %s' % pid )
    return S_OK( procStat.split( ' ' ) )

  #############################################################################
  def __getProcGroupLinux( self, pid ):
    """Returns UID for given PID.
    """
    result = shellCall( 10, 'ps --no-headers -o pgrp -p %s' % ( pid ) )
    if not result['OK']:
      if  not 'Value' in result:
        return result

    return S_OK( result['Value'][1] )

  #############################################################################
  def __checkCurrentOS( self ):
    """Checks it is possible to determine CPU consumed with this utility
       for the current OS.
    """
    localOS = None
    self.osType = platform.uname()
    if re.search( 'Darwin', self.osType[0] ):
      localOS = 'Mac'
    elif re.search( 'Windows', self.osType[0] ):
      localOS = 'Windows'
    else:
      localOS = 'Linux'
      self.log.debug( 'Will determine CPU consumed for %s flavour OS' % ( localOS ) )
    return localOS

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
